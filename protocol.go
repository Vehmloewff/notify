package main

import (
	"encoding/json"
	"fmt"
	"io"
)

const jsonRPCVersion = "2.0"

const (
	methodSend    = "notify.send"
	methodStop    = "notify.stop"
	methodCommand = "notify.command"
	methodStatus  = "notify.status"
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

type pendingCommandStatus struct {
	Command   string `json:"command"`
	Recursive bool   `json:"recursive"`
}

type instanceStatus struct {
	Name            string                 `json:"name"`
	PendingCommands []pendingCommandStatus `json:"pendingCommands"`
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type sendParams struct {
	Stdin           []byte `json:"stdin,omitempty"`
	ForwardedStderr []byte `json:"forwardedStderr,omitempty"`
}

type commandParams struct {
	Command   string `json:"command"`
	Prefix    string `json:"prefix,omitempty"`
	Recursive bool   `json:"recursive,omitempty"`
}

type stopResult struct{}

type sendResult struct{}

type commandResultAck struct{}

func writeJSONRPCRequest(w io.Writer, req rpcRequest) error {
	if req.JSONRPC == "" {
		req.JSONRPC = jsonRPCVersion
	}
	return json.NewEncoder(w).Encode(req)
}

func readJSONRPCRequest(r io.Reader) (rpcRequest, error) {
	var req rpcRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return rpcRequest{}, err
	}
	if req.JSONRPC != jsonRPCVersion {
		return rpcRequest{}, fmt.Errorf("unsupported jsonrpc version %q", req.JSONRPC)
	}
	if req.Method == "" {
		return rpcRequest{}, fmt.Errorf("missing method")
	}
	return req, nil
}

func writeJSONRPCResponse(w io.Writer, resp rpcResponse) error {
	if resp.JSONRPC == "" {
		resp.JSONRPC = jsonRPCVersion
	}
	return json.NewEncoder(w).Encode(resp)
}

func readJSONRPCResponse(r io.Reader) (rpcResponse, error) {
	var resp rpcResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return rpcResponse{}, err
	}
	if resp.JSONRPC != jsonRPCVersion {
		return rpcResponse{}, fmt.Errorf("unsupported jsonrpc version %q", resp.JSONRPC)
	}
	return resp, nil
}

func makeSuccessResponse(id uint64, result any) (rpcResponse, error) {
	body, err := json.Marshal(result)
	if err != nil {
		return rpcResponse{}, err
	}
	return rpcResponse{JSONRPC: jsonRPCVersion, ID: id, Result: body}, nil
}

func makeErrorResponse(id uint64, code int, message string) rpcResponse {
	return rpcResponse{
		JSONRPC: jsonRPCVersion,
		ID:      id,
		Error:   &rpcError{Code: code, Message: message},
	}
}

func decodeSendParams(raw json.RawMessage) (notification, error) {
	var params sendParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return notification{}, err
	}
	return notification{stdin: params.Stdin, forwardedStderr: params.ForwardedStderr}, nil
}

func decodeCommandParams(raw json.RawMessage) (commandRequest, error) {
	var params commandParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return commandRequest{}, err
	}
	if params.Command == "" {
		return commandRequest{}, fmt.Errorf("missing command")
	}
	return commandRequest{command: params.Command, prefix: params.Prefix, recursive: params.Recursive}, nil
}
