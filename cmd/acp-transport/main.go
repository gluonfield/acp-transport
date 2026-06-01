package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/gluonfield/acp-transport/jsonrpc"
	"github.com/gluonfield/acp-transport/stdio"
	"github.com/gluonfield/acp-transport/streamhttp"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(ctx, os.Args[2:])
	case "connect":
		err = runConnect(ctx, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "listen address")
	token := fs.String("token", "", "bearer token")
	agent := fs.String("agent", "", "known ACP agent shortcut: codex, claude, claude-code")
	useNpx := fs.Bool("npx", false, "allow known agent shortcuts to run adapters through npx")
	if err := fs.Parse(args); err != nil {
		return err
	}
	command, err := resolveAgentCommand(*agent, *useNpx, fs.Args())
	if err != nil {
		return err
	}

	host, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return err
	}
	effectiveToken := *token
	if effectiveToken == "" && !isLoopbackHost(host) {
		effectiveToken, err = randomToken()
		if err != nil {
			return err
		}
	}

	server := &streamhttp.Server{
		Token: effectiveToken,
		Backend: func(ctx context.Context) (jsonrpc.MessageConn, error) {
			cmd := exec.CommandContext(ctx, command[0], command[1:]...)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				return nil, err
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				return nil, err
			}
			stderr, err := cmd.StderrPipe()
			if err != nil {
				return nil, err
			}
			if err := cmd.Start(); err != nil {
				return nil, err
			}
			tail := newTailBuffer(8192)
			go func() {
				_, _ = io.Copy(io.MultiWriter(os.Stderr, tail), stderr)
			}()
			conn := newProcessConn(stdio.New(stdout, stdin), tail, func() error {
				err := cmd.Wait()
				if err != nil {
					return err
				}
				return io.EOF
			})
			return conn, nil
		},
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	addr := ln.Addr().String()
	fmt.Fprintf(os.Stderr, "ACP Streamable HTTP listening on http://%s/acp\n", addr)
	if effectiveToken != "" {
		fmt.Fprintf(os.Stderr, "Bearer token: %s\n", effectiveToken)
	}

	httpServer := &http.Server{
		Handler: h2c.NewHandler(server, &http2.Server{}),
	}
	go func() {
		<-ctx.Done()
		_ = server.Close()
		_ = httpServer.Shutdown(context.Background())
	}()
	err = httpServer.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runConnect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	endpoint := fs.String("url", "", "ACP Streamable HTTP endpoint")
	token := fs.String("token", "", "bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *endpoint == "" {
		return errors.New("connect requires -url")
	}

	parsed, err := url.Parse(*endpoint)
	if err != nil {
		return err
	}
	opts := []streamhttp.ClientOption{}
	if *token != "" {
		opts = append(opts, streamhttp.WithBearerToken(*token))
	}
	if parsed.Scheme == "http" {
		opts = append(opts, streamhttp.WithH2C())
	}

	remote, err := streamhttp.Dial(*endpoint, opts...)
	if err != nil {
		return err
	}
	local := stdio.New(os.Stdin, os.Stdout)
	return jsonrpc.Bridge(ctx, local, remote)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  acp-transport serve [--listen 127.0.0.1:0] [--token TOKEN] [--agent codex|claude] [--npx]")
	fmt.Fprintln(os.Stderr, "  acp-transport serve [--listen 127.0.0.1:0] [--token TOKEN] -- <agent command>")
	fmt.Fprintln(os.Stderr, "  acp-transport connect --url URL [--token TOKEN]")
}

func resolveAgentCommand(agent string, useNpx bool, extra []string) ([]string, error) {
	if agent == "" {
		if len(extra) == 0 {
			return nil, errors.New("serve requires --agent or an agent command after --")
		}
		return extra, nil
	}

	var command []string
	switch agent {
	case "codex":
		command = localOrNpx("codex-acp", "@zed-industries/codex-acp", useNpx)
	case "claude", "claude-code":
		command = localOrNpx("claude-code-acp", "@zed-industries/claude-code-acp", useNpx)
	case "claude-agent":
		command = localOrNpx("claude-agent-acp", "@agentclientprotocol/claude-agent-acp", useNpx)
	default:
		return nil, fmt.Errorf("unknown agent %q", agent)
	}
	if len(command) == 0 {
		return nil, fmt.Errorf("%s ACP adapter is not installed; install its binary, pass an explicit command after --, or add --npx to allow npx", agent)
	}
	return append(command, extra...), nil
}

func localOrNpx(binary string, pkg string, useNpx bool) []string {
	if path, err := exec.LookPath(binary); err == nil {
		return []string{path}
	}
	if useNpx {
		return []string{"npx", "-y", pkg}
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return strings.TrimRight(hex.EncodeToString(b[:]), "="), nil
}

type processConn struct {
	conn     jsonrpc.MessageConn
	stderr   *tailBuffer
	waitDone chan struct{}

	mu      sync.Mutex
	waitErr error
}

func newProcessConn(conn jsonrpc.MessageConn, stderr *tailBuffer, wait func() error) *processConn {
	p := &processConn{
		conn:     conn,
		stderr:   stderr,
		waitDone: make(chan struct{}),
	}
	go func() {
		err := wait()
		p.mu.Lock()
		p.waitErr = err
		p.mu.Unlock()
		close(p.waitDone)
		_ = conn.Close()
	}()
	return p
}

func (p *processConn) Send(ctx context.Context, msg *jsonrpc.Message) error {
	return p.conn.Send(ctx, msg)
}

func (p *processConn) Receive(ctx context.Context) (*jsonrpc.Message, error) {
	msg, err := p.conn.Receive(ctx)
	if err == nil {
		return msg, nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, jsonrpc.ErrClosed) {
		select {
		case <-p.waitDone:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil, p.processError(err)
}

func (p *processConn) Close() error {
	return p.conn.Close()
}

func (p *processConn) processError(fallback error) error {
	select {
	case <-p.waitDone:
	default:
		return fallback
	}
	p.mu.Lock()
	waitErr := p.waitErr
	p.mu.Unlock()
	if waitErr == nil || errors.Is(waitErr, io.EOF) {
		if errors.Is(fallback, io.EOF) || errors.Is(fallback, jsonrpc.ErrClosed) {
			return io.EOF
		}
		return fallback
	}
	tail := p.stderr.String()
	if tail != "" {
		return fmt.Errorf("agent process exited: %w; stderr: %s", waitErr, tail)
	}
	return fmt.Errorf("agent process exited: %w", waitErr)
}

type tailBuffer struct {
	mu  sync.Mutex
	max int
	buf []byte
}

func newTailBuffer(max int) *tailBuffer {
	return &tailBuffer{max: max}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		copy(b.buf, b.buf[len(b.buf)-b.max:])
		b.buf = b.buf[:b.max]
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.buf))
}
