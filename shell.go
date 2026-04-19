package main

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
	"syscall"
	"time"
)

type commandResult struct {
	stdout   []byte
	stderr   []byte
	exitCode int
	err      error
}

func runShellCapture(command string) commandResult {
	cmd := exec.Command("sh", "-c", command)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := commandResult{
		stdout: stdout.Bytes(),
		stderr: stderr.Bytes(),
		err:    err,
	}
	if err == nil {
		return result
	}
	var exitErr *exec.ExitError
	if ok := errors.As(err, &exitErr); ok {
		result.exitCode = exitErr.ExitCode()
		return result
	}
	result.exitCode = -1
	return result
}

func startShellCommand(command string, stdin []byte, stdout, stderr io.Writer) (*exec.Cmd, <-chan error, error) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	inPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}

	go func() {
		_, _ = inPipe.Write(stdin)
		_ = inPipe.Close()
	}()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	return cmd, done, nil
}

func terminateProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		if err == syscall.ESRCH {
			return nil
		}
		return err
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		return err
	}

	go func() {
		<-time.After(500 * time.Millisecond)
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}()
	return nil
}
