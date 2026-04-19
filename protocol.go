package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	messageTypeNotification   uint32 = 1
	messageTypeStop           uint32 = 2
	messageTypeCommandRequest uint32 = 3
)

type notification struct {
	stdin           []byte
	forwardedStderr []byte
}

type commandRequest struct {
	command   string
	prefix    string
	recursive bool
}

func writeNotification(w io.Writer, n notification) error {
	body := make([]byte, 8+len(n.stdin)+len(n.forwardedStderr))
	binary.BigEndian.PutUint32(body[0:4], uint32(len(n.stdin)))
	binary.BigEndian.PutUint32(body[4:8], uint32(len(n.forwardedStderr)))
	copy(body[8:], n.stdin)
	copy(body[8+len(n.stdin):], n.forwardedStderr)
	return writeFrame(w, messageTypeNotification, body)
}

func writeStop(w io.Writer) error {
	return writeFrame(w, messageTypeStop, nil)
}

func writeCommandRequest(w io.Writer, req commandRequest) error {
	commandBytes := []byte(req.command)
	prefixBytes := []byte(req.prefix)
	body := make([]byte, 9+len(commandBytes)+len(prefixBytes))
	binary.BigEndian.PutUint32(body[0:4], uint32(len(commandBytes)))
	binary.BigEndian.PutUint32(body[4:8], uint32(len(prefixBytes)))
	if req.recursive {
		body[8] = 1
	}
	copy(body[9:], commandBytes)
	copy(body[9+len(commandBytes):], prefixBytes)
	return writeFrame(w, messageTypeCommandRequest, body)
}

func writeFrame(w io.Writer, messageType uint32, body []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], messageType)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(body)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err := w.Write(body)
	return err
}

func readFrame(r io.Reader) (uint32, []byte, error) {
	br := bufio.NewReader(r)
	header := make([]byte, 8)
	if _, err := io.ReadFull(br, header); err != nil {
		return 0, nil, err
	}
	messageType := binary.BigEndian.Uint32(header[0:4])
	length := binary.BigEndian.Uint32(header[4:8])
	body := make([]byte, length)
	if _, err := io.ReadFull(br, body); err != nil {
		return 0, nil, err
	}
	return messageType, body, nil
}

func decodeNotification(body []byte) (notification, error) {
	if len(body) < 8 {
		return notification{}, fmt.Errorf("notification body too short")
	}
	stdinLen := binary.BigEndian.Uint32(body[0:4])
	stderrLen := binary.BigEndian.Uint32(body[4:8])
	expected := 8 + int(stdinLen) + int(stderrLen)
	if len(body) != expected {
		return notification{}, fmt.Errorf("notification body length mismatch")
	}
	stdinStart := 8
	stderrStart := stdinStart + int(stdinLen)
	return notification{
		stdin:           append([]byte(nil), body[stdinStart:stderrStart]...),
		forwardedStderr: append([]byte(nil), body[stderrStart:]...),
	}, nil
}

func decodeCommandRequest(body []byte) (commandRequest, error) {
	if len(body) < 9 {
		return commandRequest{}, fmt.Errorf("command request body too short")
	}
	commandLen := binary.BigEndian.Uint32(body[0:4])
	prefixLen := binary.BigEndian.Uint32(body[4:8])
	expected := 9 + int(commandLen) + int(prefixLen)
	if len(body) != expected {
		return commandRequest{}, fmt.Errorf("command request body length mismatch")
	}
	commandStart := 9
	prefixStart := commandStart + int(commandLen)
	return commandRequest{
		command:   string(body[commandStart:prefixStart]),
		prefix:    string(body[prefixStart:]),
		recursive: body[8] == 1,
	}, nil
}
