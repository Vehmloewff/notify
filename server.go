package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"slices"
	"syscall"
)

type server struct {
	name    string
	command string
	restart bool
	stdout  io.Writer
	stderr  io.Writer
}

type statusRequest struct {
	response chan instanceStatus
}

type stopRequest struct {
	commandID uint64
	response  chan error
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
	statusRequests := make(chan statusRequest)
	commandCompletions := make(chan uint64)
	stopRequests := make(chan stopRequest)
	acceptErrs := make(chan error, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go s.acceptLoop(listener, incoming, commandRequests, statusRequests, stopRequests, acceptErrs)

	var queue []notification
	var currentCmd *exec.Cmd
	var currentDone <-chan error
	terminating := false
	shuttingDown := false
	type pendingCommand struct {
		id     uint64
		status pendingCommandStatus
		cancel context.CancelFunc
	}
	var pendingCommands []pendingCommand
	nextPendingID := uint64(1)

	currentStatus := func() instanceStatus {
		status := instanceStatus{Name: s.name, PendingCommands: make([]pendingCommandStatus, 0, len(pendingCommands))}
		for _, pending := range pendingCommands {
			status.PendingCommands = append(status.PendingCommands, pending.status)
		}
		return status
	}

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

	removePendingCommand := func(id uint64) bool {
		for i, pending := range pendingCommands {
			if pending.id != id {
				continue
			}
			pendingCommands = slices.Delete(pendingCommands, i, i+1)
			return true
		}
		return false
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
			pendingID := nextPendingID
			nextPendingID++
			ctx, cancel := context.WithCancel(context.Background())
			pendingCommands = append(pendingCommands, pendingCommand{
				id:     pendingID,
				status: pendingCommandStatus{ID: pendingID, Command: req.command, Recursive: req.recursive},
				cancel: cancel,
			})
			if req.response != nil {
				req.response <- pendingID
			}
			go func(id uint64, req commandRequest) {
				defer func() { commandCompletions <- id }()
				runCommandRequestLoop(ctx, req, func(n notification) {
					incoming <- n
				})
			}(pendingID, req)
		case req := <-statusRequests:
			req.response <- currentStatus()
		case pendingID := <-commandCompletions:
			removePendingCommand(pendingID)
		case req := <-stopRequests:
			if req.commandID == 0 {
				shuttingDown = true
				for _, pending := range pendingCommands {
					pending.cancel()
				}
				_ = listener.Close()
				if currentCmd != nil && !terminating {
					terminating = true
					_ = terminateProcess(currentCmd)
				}
				req.response <- nil
				continue
			}

			var stopErr error = fmt.Errorf("pending command %d not found", req.commandID)
			for i, pending := range pendingCommands {
				if pending.id != req.commandID {
					continue
				}
				pending.cancel()
				pendingCommands = slices.Delete(pendingCommands, i, i+1)
				stopErr = nil
				break
			}
			req.response <- stopErr
		case sigErr := <-acceptErrs:
			if shuttingDown && errors.Is(sigErr, net.ErrClosed) {
				continue
			}
			return sigErr
		case <-signals:
			shuttingDown = true
			for _, pending := range pendingCommands {
				pending.cancel()
			}
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

func (s *server) acceptLoop(listener *net.UnixListener, incoming chan<- notification, commandRequests chan<- commandRequest, statusRequests chan<- statusRequest, stopRequests chan<- stopRequest, acceptErrs chan<- error) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			acceptErrs <- err
			return
		}
		go s.handleConn(conn, incoming, commandRequests, statusRequests, stopRequests, acceptErrs)
	}
}

func (s *server) handleConn(conn net.Conn, incoming chan<- notification, commandRequests chan<- commandRequest, statusRequests chan<- statusRequest, stopRequests chan<- stopRequest, acceptErrs chan<- error) {
	defer conn.Close()

	req, err := readJSONRPCRequest(conn)
	if err != nil {
		_ = writeJSONRPCResponse(conn, makeErrorResponse(0, -32700, fmt.Sprintf("failed to read request: %v", err)))
		return
	}

	var resp rpcResponse
	switch req.Method {
	case methodSend:
		n, err := decodeSendParams(req.Params)
		if err != nil {
			resp = makeErrorResponse(req.ID, -32602, fmt.Sprintf("invalid send params: %v", err))
			break
		}
		incoming <- n
		resp, err = makeSuccessResponse(req.ID, sendResult{})
		if err != nil {
			acceptErrs <- err
			return
		}
	case methodCommand:
		commandReq, err := decodeCommandParams(req.Params)
		if err != nil {
			resp = makeErrorResponse(req.ID, -32602, fmt.Sprintf("invalid command params: %v", err))
			break
		}
		commandReq.response = make(chan uint64, 1)
		commandRequests <- commandReq
		ack := commandResultAck{ID: <-commandReq.response}
		resp, err = makeSuccessResponse(req.ID, ack)
		if err != nil {
			acceptErrs <- err
			return
		}
	case methodStop:
		params, err := decodeStopParams(req.Params)
		if err != nil {
			resp = makeErrorResponse(req.ID, -32602, fmt.Sprintf("invalid stop params: %v", err))
			break
		}
		request := stopRequest{commandID: params.CommandID, response: make(chan error, 1)}
		stopRequests <- request
		if err := <-request.response; err != nil {
			resp = makeErrorResponse(req.ID, -32602, err.Error())
			break
		}
		resp, err = makeSuccessResponse(req.ID, stopResult{})
		if err != nil {
			acceptErrs <- err
			return
		}
	case methodStatus:
		request := statusRequest{response: make(chan instanceStatus, 1)}
		statusRequests <- request
		status := <-request.response
		var err error
		resp, err = makeSuccessResponse(req.ID, status)
		if err != nil {
			acceptErrs <- err
			return
		}
	default:
		resp = makeErrorResponse(req.ID, -32601, fmt.Sprintf("unknown method %q", req.Method))
	}

	if err := writeJSONRPCResponse(conn, resp); err != nil {
		acceptErrs <- fmt.Errorf("failed to write response: %w", err)
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
