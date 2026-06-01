# ACP Transport

Go implementation of ACP Streamable HTTP transport primitives.

The module is intentionally layered:

- `jsonrpc`: raw JSON-RPC 2.0 envelopes, request/response correlation helpers, and connection bridging.
- `acp`: generated Go constants and data types from the official ACP `schema.json` and `meta.json`.
- `stdio`: newline-delimited JSON-RPC transport for existing ACP subprocess agents.
- `streamhttp`: ACP Streamable HTTP server and client over HTTP/2.
- `cmd/acp-transport`: CLI bridge between stdio ACP agents and Streamable HTTP.

## Generate ACP Types

```sh
go generate ./acp
```

The generator reads `testdata/acp-schema/schema.json` and `meta.json`. Plain object and enum definitions become Go types; complex ACP unions are represented as `json.RawMessage`.

## Test

```sh
go test ./...
go test -race ./...
go vet ./...
```

## CLI

Expose a stdio ACP agent over Streamable HTTP:

```sh
go run ./cmd/acp-transport serve --listen 127.0.0.1:0 -- <agent command>
```

Use a known coding-agent adapter:

```sh
go run ./cmd/acp-transport serve --listen 127.0.0.1:8080 --agent codex
go run ./cmd/acp-transport serve --listen 127.0.0.1:8080 --agent claude
```

The built-in shortcuts look for local adapter binaries:

- `codex`: `codex-acp`
- `claude` / `claude-code`: `claude-code-acp`
- `claude-agent`: `claude-agent-acp`

They do not run `npx` unless `--npx` is passed explicitly:

```sh
go run ./cmd/acp-transport serve --listen 127.0.0.1:8080 --agent codex --npx
```

You can always bypass shortcuts and provide the exact ACP stdio agent command:

```sh
go run ./cmd/acp-transport serve --listen 127.0.0.1:8080 -- /path/to/codex-acp
```

Expose a remote Streamable HTTP ACP endpoint as local stdio:

```sh
go run ./cmd/acp-transport connect --url http://127.0.0.1:8080/acp
```

Smoke-test the transport with the bundled fake agent:

```sh
go run ./cmd/acp-transport serve --listen 127.0.0.1:8181 -- go run ./example/fake-agent
go run ./example/smoke-client -url http://127.0.0.1:8181/acp
```

The same smoke client can talk to a local stdio ACP agent directly:

```sh
go run ./example/smoke-client -cwd "$PWD" -- go run ./example/fake-agent
go run ./example/smoke-client -cwd "$PWD" -- codex-acp
go run ./example/smoke-client -cwd "$PWD" -- claude-agent-acp
```

Smoke-test a real adapter:

```sh
go run ./cmd/acp-transport serve --listen 127.0.0.1:8181 --agent codex --npx
go run ./example/smoke-client -url http://127.0.0.1:8181/acp -cwd "$PWD" -prompt "say hello"
```

The smoke client automatically calls `authenticate` when it sees an `env_var`
auth method whose documented environment variables are set. You can override
the selected ACP auth method with `-auth-method METHOD`.

Useful smoke-client flags:

- `-auth-method auto|none|METHOD`: pick an ACP auth method before `session/new`.
- `-system-prompt TEXT`: sends Claude adapter `_meta.systemPrompt` on `session/new`.
- `-permission allow|deny|cancel`: non-interactive response to permission requests.
- `-allow-write`: permits `fs/write_text_file` inside `-cwd`.

Direct HTTP test for initialize:

```sh
curl --http2-prior-knowledge -i \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}' \
  http://127.0.0.1:8181/acp
```

The server requires HTTP/2. Local `http://` usage is h2c; remote deployments should use TLS HTTP/2. A bearer token is generated automatically when serving on a non-loopback address without `--token`.

SSE streams support `Last-Event-Id` replay from the server's bounded in-memory
event buffer. If a client reconnects after the retained history is gone, the
server returns `410 Gone`; clients should recover via ACP session load/resume
when available.
