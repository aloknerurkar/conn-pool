package cpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type DummyConn struct {
	val string
}

func Dial(addr string) (Conn, error) {
	return &DummyConn{}, nil
}

func Heartbeat(c Conn) error {
	return nil
}

func Close(c Conn) error {
	return nil
}

func Test_NewClose(t *testing.T) {
	p, err := New(
		"0.0.0.0",
		Dial,
		Heartbeat,
		Close,
		10, 5, time.Second,
	)
	if err != nil {
		t.Fatal("Unable to create new pool")
	}
	if p.opened != 1 || p.idle != 1 {
		t.Fatal("Incorrect state on start")
	}
	err = p.Close()
	if err != nil {
		t.Fatal("Failed on close")
	}
	if p.opened != 0 || p.idle != 0 {
		t.Fatal("Incorrect state on stop")
	}
	select {
	case <-p.connChan:
	default:
		t.Fatal("Channel was not closed")
	}
}

func Test_Get(t *testing.T) {
	p, err := New(
		"0.0.0.0",
		Dial,
		Heartbeat,
		Close,
		10, 5, time.Second,
	)
	if err != nil {
		t.Fatal("Unable to create new pool")
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 11; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, done, err := p.Get()
			if err != nil {
				t.Fatal("Failed getting new connection", err.Error())
			}
			defer done()
			<-time.After(time.Second)
		}()
	}
	wg.Wait()
	if p.opened != 5 || p.idle != 5 {
		t.Fatal("Incorrect state on tasks done")
	}
}

func Test_Heartbeat(t *testing.T) {
	hb, dials, closes := 0, 0, 0
	p, err := New(
		"0.0.0.0",
		func(addr string) (Conn, error) {
			dials++
			return &DummyConn{val: fmt.Sprintf("%d", dials)}, nil
		},
		func(c Conn) error {
			hb++
			return nil
		},
		func(c Conn) error {
			closes++
			return nil
		},
		10, 5, time.Millisecond,
	)
	if err != nil {
		t.Fatal("Unable to create new pool")
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 11; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, done, err := p.Get()
			if err != nil {
				t.Fatal("Failed getting new connection", err.Error())
			}
			defer done()
			<-time.After(time.Second)
		}()
	}
	wg.Wait()
	if p.opened != 5 || p.idle != 5 {
		t.Fatal("Incorrect state on tasks done")
	}
	<-time.After(time.Second)
	if dials != 10 || closes != 5 || hb != 1 {
		t.Fatal("Dial, closes, heartbeat no incorrect", dials, closes, hb)
	}
	if len(p.heartbeatMap) != 5 {
		t.Fatal("Map size incorrect", len(p.heartbeatMap))
	}
}
