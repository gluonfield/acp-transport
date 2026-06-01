package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/gluonfield/acp-transport/jsonrpc"
	"github.com/gluonfield/acp-transport/stdio"
)

func main() {
	log.SetFlags(0)

	conn := stdio.New(os.Stdin, os.Stdout)
	for {
		msg, err := conn.Receive(context.Background())
		if err != nil {
			return
		}
		if !msg.IsRequest() {
			continue
		}

		switch msg.Method {
		case "initialize":
			sendResult(conn, msg, map[string]any{
				"protocolVersion": 1,
				"agentInfo": map[string]any{
					"name":    "fake-agent",
					"version": "dev",
				},
				"agentCapabilities": map[string]any{
					"loadSession": false,
				},
			})
		case "session/new":
			sendResult(conn, msg, map[string]any{
				"sessionId": "fake-session",
			})
		case "session/prompt":
			notify(conn, "session/update", map[string]any{
				"sessionId": "fake-session",
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "hello from fake agent",
					},
				},
			})
			sendResult(conn, msg, map[string]any{
				"stopReason": "end_turn",
			})
		default:
			resp, _ := jsonrpc.NewErrorResponse(*msg.ID, jsonrpc.MethodNotFound(msg.Method))
			_ = conn.Send(context.Background(), resp)
		}
	}
}

func sendResult(conn jsonrpc.MessageConn, req *jsonrpc.Message, result any) {
	resp, err := jsonrpc.NewResult(*req.ID, result)
	if err != nil {
		log.Print(err)
		return
	}
	if err := conn.Send(context.Background(), resp); err != nil {
		log.Print(err)
	}
}

func notify(conn jsonrpc.MessageConn, method string, params any) {
	msg, err := jsonrpc.NewNotification(method, params)
	if err != nil {
		log.Print(err)
		return
	}
	if _, err := json.Marshal(params); err != nil {
		log.Print(err)
		return
	}
	if err := conn.Send(context.Background(), msg); err != nil {
		log.Print(err)
	}
}
