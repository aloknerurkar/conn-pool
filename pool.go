package cpool

import (
	"context"
	"errors"
	logger "github.com/ipfs/go-log/v2"
	"sync"
	"sync/atomic"
	"time"
)

var log = logger.Logger("conn-pool")

const getConnTimeout time.Duration = time.Millisecond * 100

type Conn interface{}

type Pool struct {
	DialFn         func(addr string) (Conn, error)
	HeartBeatFn    func(Conn) error
	CloseFn        func(Conn) error
	MaxConnections int32
	MaxIdle        int32
	ServerAddr     string
	Timeout        time.Duration
	mtx            sync.Mutex
	heartbeatMap   map[Conn]time.Time
	connChan       chan Conn
	opened         int32
	idle           int32
}

type Done func()

func New(
	serverAddr string,
	dialFn func(string) (Conn, error),
	heartbeatFn func(Conn) error,
	closeFn func(Conn) error,
	maxConn int32,
	maxIdle int32,
	timeout time.Duration,
) (*Pool, error) {
	p := &Pool{
		ServerAddr:     serverAddr,
		DialFn:         dialFn,
		HeartBeatFn:    heartbeatFn,
		CloseFn:        closeFn,
		MaxConnections: maxConn,
		MaxIdle:        maxIdle,
		Timeout:        timeout,
		connChan:       make(chan Conn, maxConn),
		heartbeatMap:   make(map[Conn]time.Time, maxIdle),
	}
	_, done, err := p.Get()
	if err != nil {
		log.Errorf("Failed opening initial connection")
		p.Close()
		return nil, err
	}
	done()
	return p, nil
}

func (p *Pool) Get() (Conn, Done, error) {
	return p.GetContext(context.Background())
}

func (p *Pool) GetContext(ctx context.Context) (Conn, Done, error) {
	var newConn Conn
	var err error
retry:
	select {
	case <-ctx.Done():
		return nil, nil, errors.New("context cancelled")
	case newConn = <-p.connChan:
		idl := atomic.AddInt32(&p.idle, -1)
		log.Infof("No of idle connections: %d", idl)
	case <-time.After(getConnTimeout):
		log.Warning("Timed out getting new conn.")
		p.mtx.Lock()
		if p.opened < p.MaxConnections {
			newConn, err = p.DialFn(p.ServerAddr)
			if err != nil {
				p.mtx.Unlock()
				return nil, nil, errors.New("Failed to open new connection")
			}
			p.opened++
			log.Info(newConn)
			p.heartbeatMap[newConn] = time.Now()
			p.mtx.Unlock()
			log.Debugf("Opened new connection after timeout")
		} else {
			p.mtx.Unlock()
			log.Warnf("Already reached max connections. Retrying")
			goto retry
		}
	}

	if time.Since(p.heartbeatMap[newConn]) > p.Timeout {
		err := p.HeartBeatFn(newConn)
		if err != nil {
			// Get the lock before updating the map.
			p.mtx.Lock()
			delete(p.heartbeatMap, newConn)
			p.mtx.Unlock()
			log.Warnf("Heartbeat failed Err:%s", err.Error())
			newConn, err = p.DialFn(p.ServerAddr)
			if err != nil {
				log.Errorf("Failed to create new conn Err:%s", err.Error())
				return nil, nil, err
			}
		}
		// Get the lock before updating the map.
		p.mtx.Lock()
		p.heartbeatMap[newConn] = time.Now()
		p.mtx.Unlock()
	}
	return newConn, func() { p.connDone(newConn) }, nil
}

func (p *Pool) connDone(conn Conn) {
	if p.idle == p.MaxIdle {
		p.CloseFn(conn)
		opnd := atomic.AddInt32(&p.opened, -1)
		p.mtx.Lock()
		delete(p.heartbeatMap, conn)
		p.mtx.Unlock()
		log.Infof("Closing connection as passed max idle limit Opened:%d", opnd)
		return
	}
	idl := atomic.AddInt32(&p.idle, 1)
	log.Infof("No of idle connections: %d", idl)
	p.connChan <- conn
}

func (p *Pool) Close() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	close(p.connChan)

	for conn := range p.connChan {
		p.CloseFn(conn)
	}
	p.opened = 0
	p.idle = 0
	return nil
}
