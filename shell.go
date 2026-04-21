package main

import (
	"bytes"
	"context"
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
	canceled bool
}

func runShellCapture(command string) commandResult {
	return runShellCaptureContext(context.Background(), command)
}

func runShellCaptureContext(ctx context.Context, command string) commandResult {
	cmd := exec.Command("sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return commandResult{stdout: stdout.Bytes(), stderr: stderr.Bytes(), err: err, exitCode: -1}
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- cmd.Wait()
	}()

	select {
	case err := <-waitErr:
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
	case <-ctx.Done():
		_ = terminateProcess(cmd)
		<-waitErr
		return commandResult{
			stdout:   stdout.Bytes(),
			stderr:   stderr.Bytes(),
			err:      ctx.Err(),
			exitCode: -1,
			canceled: true,
		}
	}
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
