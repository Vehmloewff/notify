package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestPreparePayloadTruncatesAndPersistsFullPayload(t *testing.T) {
	body := bytes.Repeat([]byte("a"), maxNotificationSize+100)
	payload, err := preparePayload("prefix-", body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	idx := strings.Index(text, "Notification over 50kb and truncated. View the entire notification here: ")
	if idx == -1 {
		t.Fatalf("truncation notice missing from payload: %q", text[len(text)-120:])
	}
	path := text[idx+len("Notification over 50kb and truncated. View the entire notification here: "):]
	expectedPrefix := filepath.Join(os.TempDir(), "notify-")
	if !strings.HasPrefix(path, expectedPrefix) {
		t.Fatalf("unexpected temp file path %q", path)
	}
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(full) != "prefix-"+string(body) {
		t.Fatalf("temp file did not contain full payload")
	}
}

func TestProtocolRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	params := sendParams{Stdin: []byte("hello\x00world"), ForwardedStderr: []byte("warn\n")}
	paramsBody, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONRPCRequest(&buf, rpcRequest{ID: 7, Method: methodSend, Params: paramsBody}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(buf.String(), "\n") {
		t.Fatalf("expected newline-delimited request, got %q", buf.String())
	}
	decodedReq, err := readJSONRPCRequest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decodedReq.ID != 7 || decodedReq.Method != methodSend {
		t.Fatalf("unexpected request: %#v", decodedReq)
	}
	decodedNotification, err := decodeSendParams(decodedReq.Params)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decodedNotification.stdin, params.Stdin) || !bytes.Equal(decodedNotification.forwardedStderr, params.ForwardedStderr) {
		t.Fatalf("decoded notification mismatch: %#v", decodedNotification)
	}

	buf.Reset()
	response, err := makeSuccessResponse(7, instanceStatus{
		Name:            "demo",
		PendingCommands: []pendingCommandStatus{{ID: 1, Command: "printf hi", Recursive: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONRPCResponse(&buf, response); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(buf.String(), "\n") {
		t.Fatalf("expected newline-delimited response, got %q", buf.String())
	}
	decodedResp, err := readJSONRPCResponse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decodedResp.ID != 7 {
		t.Fatalf("unexpected response id: %d", decodedResp.ID)
	}
	var status instanceStatus
	if err := json.Unmarshal(decodedResp.Result, &status); err != nil {
		t.Fatal(err)
	}
	if status.Name != "demo" || len(status.PendingCommands) != 1 || status.PendingCommands[0].ID != 1 || status.PendingCommands[0].Command != "printf hi" || !status.PendingCommands[0].Recursive {
		t.Fatalf("decoded status mismatch: %#v", status)
	}
}

func TestQueueModeFIFOAndFreshProcess(t *testing.T) {
	tmp := t.TempDir()
	outFile := filepath.Join(tmp, "out.log")
	pidFile := filepath.Join(tmp, "pids.log")
	name := uniqueName("fifo")
	stderr := &lockedBuffer{}

	command := fmt.Sprintf("payload=$(cat); printf '%%s\n' \"$payload\" >> %s; printf '%%s\n' $$ >> %s", shellQuote(outFile), shellQuote(pidFile))
	stopServer := startTestServer(t, name, command, false, stderr)
	defer stopServer()

	mustSendStdin(t, name, "first")
	mustSendStdin(t, name, "second")

	waitForContains(t, outFile, "first\nsecond\n")
	waitForLines(t, pidFile, 2)
	lines := readLines(t, pidFile)
	if len(lines) != 2 || lines[0] == lines[1] {
		t.Fatalf("expected distinct shell pids, got %v", lines)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("unexpected stderr: %q", got)
	}
}

func TestRestartModeKillsCurrentAndPreservesQueue(t *testing.T) {
	tmp := t.TempDir()
	logFile := filepath.Join(tmp, "restart.log")
	name := uniqueName("restart")
	stderr := &lockedBuffer{}

	command := fmt.Sprintf("payload=$(cat); trap 'printf \"killed:%%s\\n\" \"$payload\" >> %s; exit 0' TERM; printf 'start:%%s\\n' \"$payload\" >> %s; sleep 2; printf 'done:%%s\\n' \"$payload\" >> %s", shellQuote(logFile), shellQuote(logFile), shellQuote(logFile))
	stopServer := startTestServer(t, name, command, true, stderr)
	defer stopServer()

	mustSendStdin(t, name, "one")
	time.Sleep(200 * time.Millisecond)
	mustSendStdin(t, name, "two")

	waitForContains(t, logFile, "start:one\n")
	waitForContains(t, logFile, "killed:one\n")
	waitForContains(t, logFile, "start:two\n")
	waitForContains(t, logFile, "done:two\n")
}

func TestSendCommandForwardsStderrToInstanceStderr(t *testing.T) {
	name := uniqueName("stderr")
	stderr := &lockedBuffer{}
	stopServer := startTestServer(t, name, "cat >/dev/null", false, stderr)
	defer stopServer()

	err := executeCLI([]string{"command", "--name", name, "printf data; printf warn >&2"}, strings.NewReader(""), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	waitForBufferContains(t, stderr, "warn")
}

func TestSendCommandIsFireAndForget(t *testing.T) {
	tmp := t.TempDir()
	outFile := filepath.Join(tmp, "async.log")
	name := uniqueName("async")
	stderr := &lockedBuffer{}
	command := fmt.Sprintf("payload=$(cat); printf '%%s\n' \"$payload\" >> %s", shellQuote(outFile))
	stopServer := startTestServer(t, name, command, false, stderr)
	defer stopServer()

	started := time.Now()
	err := executeCLI([]string{"command", "--name", name, "sleep 1; printf done"}, strings.NewReader(""), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("command --command blocked too long: %v", elapsed)
	}
	waitForContains(t, outFile, "done\n")
}

func TestRecursiveModeStopsAfterThreeConsecutiveFailures(t *testing.T) {
	tmp := t.TempDir()
	outFile := filepath.Join(tmp, "recursive.log")
	name := uniqueName("recursive")
	stderr := &lockedBuffer{}
	command := fmt.Sprintf("payload=$(cat); printf '%%s\n' \"$payload\" >> %s", shellQuote(outFile))
	stopServer := startTestServer(t, name, command, false, stderr)
	defer stopServer()

	err := executeCLI([]string{"command", "--name", name, "--recursive", "printf loop; exit 1"}, strings.NewReader(""), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	waitForLines(t, outFile, 3)
	if lines := readLines(t, outFile); len(lines) != 3 {
		t.Fatalf("expected 3 notifications, got %v", lines)
	}
}

func TestStartRemovesStaleSocket(t *testing.T) {
	name := uniqueName("stale")
	path := socketPath(name)
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	stderr := &lockedBuffer{}
	stopServer := startTestServer(t, name, "cat >/dev/null", false, stderr)
	defer stopServer()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected socket to exist after start: %v", err)
	}
}

func TestSendUsesOnlyRunningInstanceAutomatically(t *testing.T) {
	tmp := t.TempDir()
	outFile := filepath.Join(tmp, "auto.log")
	name := uniqueName("auto")
	stderr := &lockedBuffer{}
	command := fmt.Sprintf("payload=$(cat); printf '%%s\n' \"$payload\" >> %s", shellQuote(outFile))
	stopServer := startTestServer(t, name, command, false, stderr)
	defer stopServer()

	if err := executeCLI([]string{"send"}, strings.NewReader("hello"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	waitForContains(t, outFile, "hello\n")
}

func TestStopUsesOnlyRunningInstanceAutomatically(t *testing.T) {
	name := uniqueName("autostop")
	stderr := &lockedBuffer{}
	cleanup := startTestServer(t, name, "sleep 10", false, stderr)
	_ = cleanup
	if err := executeCLI([]string{"stop"}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	waitForSocketRemoval(t, name)
}

func TestSendWithoutNameFailsWhenMultipleInstancesExist(t *testing.T) {
	stderr := &lockedBuffer{}
	stopA := startTestServer(t, uniqueName("multi-a"), "cat >/dev/null", false, stderr)
	defer stopA()
	stopB := startTestServer(t, uniqueName("multi-b"), "cat >/dev/null", false, stderr)
	defer stopB()

	err := executeCLI([]string{"send"}, strings.NewReader("hello"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "multiple notify instances found") {
		t.Fatalf("expected multiple instance error, got %v", err)
	}
}

func TestListShowsRunningInstances(t *testing.T) {
	stderr := &lockedBuffer{}
	nameA := uniqueName("list-a")
	nameB := uniqueName("list-b")
	stopA := startTestServer(t, nameA, "cat >/dev/null", false, stderr)
	defer stopA()
	stopB := startTestServer(t, nameB, "cat >/dev/null", false, stderr)
	defer stopB()

	var stdout bytes.Buffer
	if err := executeCLI([]string{"list"}, strings.NewReader(""), &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	out := stdout.String()
	if !strings.Contains(out, nameA+"\n") || !strings.Contains(out, nameB+"\n") {
		t.Fatalf("expected list output to contain %q and %q, got %q", nameA, nameB, out)
	}
}

func TestListShowsPendingCommandsPerInstance(t *testing.T) {
	stderr := &lockedBuffer{}
	name := uniqueName("list-pending")
	stopServer := startTestServer(t, name, "cat >/dev/null", false, stderr)
	defer stopServer()

	if err := executeCLI([]string{"command", "--name", name, "sleep 2; printf done"}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := executeCLI([]string{"command", "--name", name, "--recursive", "sleep 2; exit 1"}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}

	waitForPendingCommands(t, name, 2)

	var stdout bytes.Buffer
	if err := executeCLI([]string{"list"}, strings.NewReader(""), &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	out := stdout.String()
	if !strings.Contains(out, name+"\n") {
		t.Fatalf("expected list output to contain instance %q, got %q", name, out)
	}
	if !strings.Contains(out, "  - [1] sleep 2; printf done\n") {
		t.Fatalf("expected list output to contain non-recursive pending command with id, got %q", out)
	}
	if !strings.Contains(out, "  - [2] sleep 2; exit 1 (recursive)\n") {
		t.Fatalf("expected list output to contain recursive pending command with id, got %q", out)
	}
}

func TestStopSpecificPendingCommand(t *testing.T) {
	stderr := &lockedBuffer{}
	name := uniqueName("stop-command")
	stopServer := startTestServer(t, name, "cat >/dev/null", false, stderr)
	defer stopServer()

	if err := executeCLI([]string{"command", "--name", name, "sleep 10; printf first"}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := executeCLI([]string{"command", "--name", name, "sleep 10; printf second"}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}

	waitForPendingCommands(t, name, 2)

	if err := executeCLI([]string{"stop", "--name", name, "--command", "1"}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}

	waitForPendingCommandIDs(t, name, []uint64{2})
	if _, err := queryInstanceStatus(name); err != nil {
		t.Fatalf("expected instance %q to still be running: %v", name, err)
	}
}

func TestStopSpecificPendingCommandFailsWhenMissing(t *testing.T) {
	stderr := &lockedBuffer{}
	name := uniqueName("stop-missing")
	stopServer := startTestServer(t, name, "cat >/dev/null", false, stderr)
	defer stopServer()

	err := executeCLI([]string{"stop", "--name", name, "--command", "99"}, strings.NewReader(""), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "pending command 99 not found") {
		t.Fatalf("expected missing pending command error, got %v", err)
	}
}

func TestStartGeneratesRandomNameAndPrintsIt(t *testing.T) {
	stderr := &lockedBuffer{}
	var stdout bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- executeCLI([]string{"start", "cat >/dev/null"}, strings.NewReader(""), &stdout, stderr)
	}()

	name := waitForPrintedName(t, &stdout)
	waitForSocket(t, name)
	if err := executeCLI([]string{"stop", "--name", name}, strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for generated-name server shutdown")
	}
}

func startTestServer(t *testing.T, name, command string, restart bool, stderr io.Writer) func() {
	t.Helper()
	errCh := make(chan error, 1)
	args := []string{"start", "--name", name}
	if restart {
		args = append(args, "--restart")
	}
	args = append(args, command)
	go func() {
		errCh <- executeCLI(args, strings.NewReader(""), io.Discard, stderr)
	}()
	waitForSocket(t, name)
	return func() {
		err := executeCLI([]string{"stop", "--name", name}, strings.NewReader(""), io.Discard, io.Discard)
		if err != nil && !strings.Contains(err.Error(), "failed to connect") {
			t.Fatalf("stop failed: %v", err)
		}
		select {
		case runErr := <-errCh:
			if runErr != nil {
				t.Fatalf("server exited with error: %v", runErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for server shutdown")
		}
	}
}

func mustSendStdin(t *testing.T, name, body string) {
	t.Helper()
	if err := executeCLI([]string{"send", "--name", name}, strings.NewReader(body), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func waitForSocket(t *testing.T, name string) {
	t.Helper()
	path := socketPath(name)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("socket %s was not created", path)
}

func waitForSocketRemoval(t *testing.T, name string) {
	t.Helper()
	path := socketPath(name)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("socket %s was not removed", path)
}

func waitForContains(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(path)
		if strings.Contains(string(data), want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	t.Fatalf("file %s never contained %q; got %q", path, want, string(data))
}

func waitForLines(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(readLinesFile(path)) >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("file %s never reached %d lines", path, want)
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	return readLinesFile(path)
}

func readLinesFile(path string) []string {
	data, _ := os.ReadFile(path)
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func waitForBufferContains(t *testing.T, buf *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("buffer never contained %q; got %q", want, buf.String())
}

func waitForPendingCommands(t *testing.T, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := queryInstanceStatus(name)
		if err == nil && len(status.PendingCommands) >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	status, err := queryInstanceStatus(name)
	if err != nil {
		t.Fatalf("failed reading status for %q: %v", name, err)
	}
	t.Fatalf("instance %q never reached %d pending commands; got %d", name, want, len(status.PendingCommands))
}

func waitForPendingCommandIDs(t *testing.T, name string, want []uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := queryInstanceStatus(name)
		if err == nil && len(status.PendingCommands) == len(want) {
			matches := true
			for i, pending := range status.PendingCommands {
				if pending.ID != want[i] {
					matches = false
					break
				}
			}
			if matches {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	status, err := queryInstanceStatus(name)
	if err != nil {
		t.Fatalf("failed reading status for %q: %v", name, err)
	}
	t.Fatalf("instance %q never reached pending command ids %v; got %#v", name, want, status.PendingCommands)
}

func waitForPrintedName(t *testing.T, stdout *bytes.Buffer) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		text := strings.TrimSpace(stdout.String())
		if text != "" {
			return strings.Split(text, "\n")[0]
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for printed instance name")
	return ""
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%x", prefix, uint32(time.Now().UnixNano()))
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
