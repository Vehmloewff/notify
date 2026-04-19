package main

import (
	"fmt"
	"net"
)

func sendNotification(name string, n notification) error {
	conn, err := net.Dial("unix", socketPath(name))
	if err != nil {
		return fmt.Errorf("failed to connect to instance %q: %w", name, err)
	}
	defer conn.Close()
	if err := writeNotification(conn, n); err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}
	return nil
}

func sendStop(name string) error {
	conn, err := net.Dial("unix", socketPath(name))
	if err != nil {
		return fmt.Errorf("failed to connect to instance %q: %w", name, err)
	}
	defer conn.Close()
	if err := writeStop(conn); err != nil {
		return fmt.Errorf("failed to send stop signal: %w", err)
	}
	return nil
}

func sendCommandRequest(name string, req commandRequest) error {
	conn, err := net.Dial("unix", socketPath(name))
	if err != nil {
		return fmt.Errorf("failed to connect to instance %q: %w", name, err)
	}
	defer conn.Close()
	if err := writeCommandRequest(conn, req); err != nil {
		return fmt.Errorf("failed to send command request: %w", err)
	}
	return nil
}
