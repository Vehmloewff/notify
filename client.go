package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
)

var nextRPCID uint64

func sendNotification(name string, n notification) error {
	_, err := callRPC(name, methodSend, sendParams{Stdin: n.stdin, ForwardedStderr: n.forwardedStderr}, nil)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}
	return nil
}

func sendStop(name string, commandID uint64) error {
	_, err := callRPC(name, methodStop, stopParams{CommandID: commandID}, nil)
	if err != nil {
		return fmt.Errorf("failed to send stop signal: %w", err)
	}
	return nil
}

func sendCommandRequest(name string, req commandRequest) (uint64, error) {
	var ack commandResultAck
	_, err := callRPC(name, methodCommand, commandParams{Command: req.command, Prefix: req.prefix, Recursive: req.recursive}, &ack)
	if err != nil {
		return 0, fmt.Errorf("failed to send command request: %w", err)
	}
	return ack.ID, nil
}

func queryInstanceStatus(name string) (instanceStatus, error) {
	var status instanceStatus
	_, err := callRPC(name, methodStatus, struct{}{}, &status)
	if err != nil {
		return instanceStatus{}, fmt.Errorf("failed to query status for %q: %w", name, err)
	}
	return status, nil
}

func callRPC(name, method string, params any, result any) (uint64, error) {
	conn, err := net.Dial("unix", socketPath(name))
	if err != nil {
		return 0, fmt.Errorf("failed to connect to instance %q: %w", name, err)
	}
	defer conn.Close()

	id := atomic.AddUint64(&nextRPCID, 1)
	paramsBody, err := json.Marshal(params)
	if err != nil {
		return 0, err
	}
	if err := writeJSONRPCRequest(conn, rpcRequest{ID: id, Method: method, Params: paramsBody}); err != nil {
		return 0, err
	}

	resp, err := readJSONRPCResponse(conn)
	if err != nil {
		return 0, err
	}
	if resp.ID != id {
		return 0, fmt.Errorf("unexpected response id %d", resp.ID)
	}
	if resp.Error != nil {
		return 0, fmt.Errorf("%s", resp.Error.Message)
	}
	if result != nil && len(resp.Result) > 0 {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return 0, err
		}
	}
	return id, nil
}
