package jsonrpc

import (
	"context"
)

func Bridge(ctx context.Context, a MessageConn, b MessageConn) error {
	errCh := make(chan error, 2)
	go bridgeOneWay(ctx, a, b, errCh)
	go bridgeOneWay(ctx, b, a, errCh)

	err := <-errCh
	_ = a.Close()
	_ = b.Close()
	if IsClosed(err) {
		return nil
	}
	return err
}

func bridgeOneWay(ctx context.Context, src MessageConn, dst MessageConn, errCh chan<- error) {
	for {
		msg, err := src.Receive(ctx)
		if err != nil {
			errCh <- err
			return
		}
		if err := dst.Send(ctx, msg); err != nil {
			errCh <- err
			return
		}
	}
}
