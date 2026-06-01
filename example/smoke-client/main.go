package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gluonfield/acp-transport/acp"
	"github.com/gluonfield/acp-transport/jsonrpc"
	"github.com/gluonfield/acp-transport/stdio"
	"github.com/gluonfield/acp-transport/streamhttp"
)

type hostConfig struct {
	cwd        string
	permission string
	allowWrite bool
}

func main() {
	log.SetFlags(0)

	endpoint := flag.String("url", "http://127.0.0.1:8080/acp", "ACP Streamable HTTP endpoint")
	token := flag.String("token", "", "bearer token")
	cwd := flag.String("cwd", ".", "session working directory")
	prompt := flag.String("prompt", "say hello in one sentence", "prompt to send")
	authMethod := flag.String("auth-method", "auto", "auth method id to call before session/new; use auto, none, or a concrete method id")
	systemPrompt := flag.String("system-prompt", "", "optional session _meta.systemPrompt")
	permission := flag.String("permission", "deny", "permission policy for session/request_permission: allow, deny, or cancel")
	allowWrite := flag.Bool("allow-write", false, "allow fs/write_text_file inside -cwd")
	timeout := flag.Duration("timeout", 60*time.Second, "overall smoke test timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client, err := openConn(ctx, *endpoint, *token, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	host := hostConfig{cwd: *cwd, permission: *permission, allowWrite: *allowWrite}
	initResp := call(ctx, client, host, `1`, acp.AgentMethodInitialize, map[string]any{"protocolVersion": 1})
	if methodID, ok := selectAuthMethod(initResp.Result, *authMethod); ok {
		call(ctx, client, host, `2`, acp.AgentMethodAuthenticate, map[string]any{"methodId": methodID})
	}
	sessionParams := map[string]any{"cwd": *cwd, "mcpServers": []any{}}
	if *systemPrompt != "" {
		sessionParams["_meta"] = map[string]any{"systemPrompt": *systemPrompt}
	}
	sessionResp := call(ctx, client, host, `3`, acp.AgentMethodSessionNew, sessionParams)
	sessionID := jsonrpc.SessionIDFromMessage(sessionResp)
	if sessionID == "" {
		log.Fatal("session/new response did not include sessionId")
	}
	call(ctx, client, host, `4`, acp.AgentMethodSessionPrompt, map[string]any{
		"sessionId": sessionID,
		"prompt": []any{
			map[string]any{"type": "text", "text": *prompt},
		},
	})
}

func openConn(ctx context.Context, endpoint string, token string, command []string) (jsonrpc.MessageConn, error) {
	if len(command) > 0 {
		return openStdioConn(ctx, command)
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	opts := []streamhttp.ClientOption{}
	if parsed.Scheme == "http" {
		opts = append(opts, streamhttp.WithH2C())
	}
	if token != "" {
		opts = append(opts, streamhttp.WithBearerToken(token))
	}

	return streamhttp.Dial(endpoint, opts...)
}

func openStdioConn(ctx context.Context, command []string) (jsonrpc.MessageConn, error) {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	conn := stdio.New(stdout, stdin)
	go func() {
		_ = cmd.Wait()
		_ = conn.Close()
	}()
	return conn, nil
}

func call(ctx context.Context, client jsonrpc.MessageConn, host hostConfig, id string, method string, params any) *jsonrpc.Message {
	req, err := jsonrpc.NewRequest(json.RawMessage(id), method, params)
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Send(ctx, req); err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := client.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}
		printMessage(msg)
		if msg.IsRequest() {
			handleClientRequest(ctx, client, host, msg)
			continue
		}
		if msg.IsResponse() && msg.ID != nil && string(*msg.ID) == id {
			if msg.Error != nil {
				log.Fatalf("%s failed: %v", method, msg.Error)
			}
			return msg
		}
	}
}

func handleClientRequest(ctx context.Context, client jsonrpc.MessageConn, host hostConfig, msg *jsonrpc.Message) {
	var result any
	var rpcErr *jsonrpc.Error

	switch msg.Method {
	case acp.ClientMethodSessionRequestPermission:
		result, rpcErr = requestPermissionResult(msg.Params, host.permission)
	case acp.ClientMethodFSReadTextFile:
		result, rpcErr = readTextFileResult(msg.Params, host.cwd)
	case acp.ClientMethodFSWriteTextFile:
		result, rpcErr = writeTextFileResult(msg.Params, host.cwd, host.allowWrite)
	case acp.ClientMethodTerminalKill, acp.ClientMethodTerminalRelease:
		result = map[string]any{}
	case acp.ClientMethodTerminalCreate, acp.ClientMethodTerminalOutput, acp.ClientMethodTerminalWaitForExit:
		rpcErr = jsonrpc.InternalError("terminal support is disabled in smoke-client", nil)
	default:
		rpcErr = jsonrpc.MethodNotFound(msg.Method)
	}

	var resp *jsonrpc.Message
	var err error
	if rpcErr != nil {
		resp, err = jsonrpc.NewErrorResponse(*msg.ID, rpcErr)
	} else {
		resp, err = jsonrpc.NewResult(*msg.ID, result)
	}
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Send(ctx, resp); err != nil {
		log.Fatal(err)
	}
}

func selectAuthMethod(result json.RawMessage, wanted string) (string, bool) {
	if wanted == "" || wanted == "none" {
		return "", false
	}
	if wanted != "auto" {
		return wanted, true
	}

	var init struct {
		AuthMethods []struct {
			Type string `json:"type"`
			ID   string `json:"id"`
			Vars []struct {
				Name string `json:"name"`
			} `json:"vars"`
		} `json:"authMethods"`
	}
	if err := json.Unmarshal(result, &init); err != nil {
		return "", false
	}
	for _, method := range init.AuthMethods {
		if method.Type != "env_var" && len(method.Vars) == 0 {
			continue
		}
		allSet := len(method.Vars) > 0
		for _, v := range method.Vars {
			if os.Getenv(v.Name) == "" {
				allSet = false
				break
			}
		}
		if allSet {
			return method.ID, true
		}
	}
	return "", false
}

func requestPermissionResult(raw json.RawMessage, policy string) (any, *jsonrpc.Error) {
	var req acp.RequestPermissionRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, jsonrpc.InvalidParams("invalid permission request", map[string]any{"error": err.Error()})
	}
	if policy == "cancel" {
		return permissionCancelled(), nil
	}
	optionID := selectPermissionOption(req.Options, policy)
	if optionID == "" {
		return permissionCancelled(), nil
	}
	return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": optionID}}, nil
}

func permissionCancelled() map[string]any {
	return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}
}

func selectPermissionOption(options []acp.PermissionOption, policy string) string {
	needAllow := policy == "allow"
	for _, opt := range options {
		text := strings.ToLower(string(opt.OptionID) + " " + opt.Name)
		if needAllow && (strings.Contains(text, "allow") || strings.Contains(text, "approve")) {
			return string(opt.OptionID)
		}
		if !needAllow && (strings.Contains(text, "deny") || strings.Contains(text, "reject")) {
			return string(opt.OptionID)
		}
	}
	if needAllow && len(options) > 0 {
		return string(options[0].OptionID)
	}
	return ""
}

func readTextFileResult(raw json.RawMessage, root string) (any, *jsonrpc.Error) {
	var req acp.ReadTextFileRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, jsonrpc.InvalidParams("invalid read request", map[string]any{"error": err.Error()})
	}
	path, rpcErr := safePath(root, req.Path)
	if rpcErr != nil {
		return nil, rpcErr
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, jsonrpc.InternalError("read failed", map[string]any{"error": err.Error()})
	}
	content := string(b)
	if req.Line > 1 {
		lines := strings.SplitAfter(content, "\n")
		if req.Line-1 < len(lines) {
			content = strings.Join(lines[req.Line-1:], "")
		} else {
			content = ""
		}
	}
	if req.Limit > 0 && len(content) > req.Limit {
		content = content[:req.Limit]
	}
	return map[string]any{"content": content}, nil
}

func writeTextFileResult(raw json.RawMessage, root string, allow bool) (any, *jsonrpc.Error) {
	if !allow {
		return nil, jsonrpc.InternalError("write support is disabled; pass -allow-write to enable it", nil)
	}
	var req acp.WriteTextFileRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, jsonrpc.InvalidParams("invalid write request", map[string]any{"error": err.Error()})
	}
	path, rpcErr := safePath(root, req.Path)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if err := os.WriteFile(path, []byte(req.Content), 0o644); err != nil {
		return nil, jsonrpc.InternalError("write failed", map[string]any{"error": err.Error()})
	}
	return map[string]any{}, nil
}

func safePath(root string, value string) (string, *jsonrpc.Error) {
	if value == "" {
		return "", jsonrpc.InvalidParams("path is required", nil)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", jsonrpc.InternalError("invalid root", map[string]any{"error": err.Error()})
	}
	path := value
	if !filepath.IsAbs(path) {
		path = filepath.Join(rootAbs, path)
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return "", jsonrpc.InvalidParams("invalid path", map[string]any{"error": err.Error()})
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", jsonrpc.InvalidParams("path is outside -cwd", map[string]any{"path": value})
	}
	return pathAbs, nil
}

func printMessage(msg *jsonrpc.Message) {
	b, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
}
