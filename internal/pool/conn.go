package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/param-jeet-singh/go-redis/v9/internal/proto"
)

var noDeadline = time.Time{}

type Conn struct {
	usedAt  int64 // atomic
	netConn net.Conn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	// Byte counters for debugging
	bytesRead    int64 // atomic
	bytesWritten int64 // atomic

	Inited    bool
	pooled    bool
	createdAt time.Time
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.wr = proto.NewWriter(cn.bw)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

// BytesRead returns the total bytes read by this connection
func (cn *Conn) BytesRead() int64 {
	return atomic.LoadInt64(&cn.bytesRead)
}

// BytesWritten returns the total bytes written by this connection
func (cn *Conn) BytesWritten() int64 {
	return atomic.LoadInt64(&cn.bytesWritten)
}

// ResetByteCounters resets the byte counters (useful for tracking per-operation stats)
func (cn *Conn) ResetByteCounters() {
	atomic.StoreInt64(&cn.bytesRead, 0)
	atomic.StoreInt64(&cn.bytesWritten, 0)
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	n, err := cn.netConn.Write(b)
	if n > 0 {
		atomic.AddInt64(&cn.bytesWritten, int64(n))
	}
	return n, err
}

func (cn *Conn) RemoteAddr() net.Addr {
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

func (cn *Conn) WithReader(
	ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error,
) error {
	if timeout >= 0 {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}
	preBuffered := cn.rd.Buffered()
	err := fn(cn.rd)
	postBuffered := cn.rd.Buffered()
	// Estimate bytes read (not perfect but gives an indication)
	if preBuffered > postBuffered {
		bytesConsumed := preBuffered - postBuffered
		atomic.AddInt64(&cn.bytesRead, int64(bytesConsumed))
	}
	return err
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	if timeout >= 0 {
		if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}

	if cn.bw.Buffered() > 0 {
		cn.bw.Reset(cn.netConn)
	}

	if err := fn(cn.wr); err != nil {
		return err
	}

	return cn.bw.Flush()
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
