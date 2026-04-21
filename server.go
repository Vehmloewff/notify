package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"syscall"
)

type server struct {
	name    string
	command string
	restart bool
	stdout  io.Writer
	stderr  io.Writer
}

func (s *server) run() error {
	listener, err := prepareListener(s.name)
	if err != nil {
		return err
	}
	return s.runWithListener(listener)
}

func (s *server) runWithListener(listener *net.UnixListener) error {
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketPath(s.name))
	}()

	incoming := make(chan notification)
	commandRequests := make(chan commandRequest)
	stopRequests := make(chan struct{}, 1)
	acceptErrs := make(chan error, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go s.acceptLoop(listener, incoming, commandRequests, stopRequests, acceptErrs)

	var queue []notification
	var currentCmd *exec.Cmd
	var currentDone <-chan error
	terminating := false
	shuttingDown := false

	startNext := func() error {
		if shuttingDown || currentCmd != nil || len(queue) == 0 {
			return nil
		}
		next := queue[0]
		queue = queue[1:]
		cmd, done, err := startShellCommand(s.command, next.stdin, s.stdout, s.stderr)
		if err != nil {
			return err
		}
		currentCmd = cmd
		currentDone = done
		terminating = false
		return nil
	}

	for {
		if err := startNext(); err != nil {
			return err
		}
		if shuttingDown && currentCmd == nil {
			return nil
		}

		select {
		case n := <-incoming:
			if len(n.forwardedStderr) > 0 {
				_, _ = s.stderr.Write(n.forwardedStderr)
			}
			queue = append(queue, n)
			if s.restart && currentCmd != nil && !terminating {
				terminating = true
				_ = terminateProcess(currentCmd)
			}
		case req := <-commandRequests:
			go runCommandRequestLoop(req, func(n notification) {
				incoming <- n
			})
		case <-stopRequests:
			shuttingDown = true
			_ = listener.Close()
			if currentCmd != nil && !terminating {
				terminating = true
				_ = terminateProcess(currentCmd)
			}
		case sigErr := <-acceptErrs:
			if shuttingDown && errors.Is(sigErr, net.ErrClosed) {
				continue
			}
			return sigErr
		case <-signals:
			shuttingDown = true
			_ = listener.Close()
			if currentCmd != nil && !terminating {
				terminating = true
				_ = terminateProcess(currentCmd)
			}
		case err := <-currentDone:
			_ = err
			currentCmd = nil
			currentDone = nil
			terminating = false
		}
	}
}

func (s *server) acceptLoop(listener *net.UnixListener, incoming chan<- notification, commandRequests chan<- commandRequest, stopRequests chan<- struct{}, acceptErrs chan<- error) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			acceptErrs <- err
			return
		}
		messageType, body, err := readFrame(conn)
		_ = conn.Close()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				continue
			}
			acceptErrs <- fmt.Errorf("failed to read message: %w", err)
			return
		}
		switch messageType {
		case messageTypeNotification:
			n, err := decodeNotification(body)
			if err != nil {
				acceptErrs <- err
				return
			}
			incoming <- n
		case messageTypeCommandRequest:
			req, err := decodeCommandRequest(body)
			if err != nil {
				acceptErrs <- err
				return
			}
			commandRequests <- req
		case messageTypeStop:
			select {
			case stopRequests <- struct{}{}:
			default:
			}
		default:
			acceptErrs <- fmt.Errorf("unknown message type: %d", messageType)
			return
		}
	}
}

func prepareListener(name string) (*net.UnixListener, error) {
	path := socketPath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("failed to create socket directory: %w", err)
	}
	if _, err := os.Stat(path); err == nil {
		conn, dialErr := net.Dial("unix", path)
		if dialErr == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("instance %q already exists", name)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("failed to remove stale socket %s: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	addr := &net.UnixAddr{Name: path, Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return listener, nil
}

func socketPath(name string) string {
	return filepath.Join(socketDir(), name+".sock")
}

func socketDir() string {
	username := os.Getenv("USER")
	if username == "" {
		if currentUser, err := user.Current(); err == nil && currentUser.Username != "" {
			username = currentUser.Username
		}
	}
	if username == "" {
		username = "unknown"
	}
	return filepath.Join(os.TempDir(), "notify-"+username)
}
