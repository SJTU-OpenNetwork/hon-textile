package stream

import (
	"errors"
	"sync"
	"time"
)

type ConnEle interface {
	Close() error
}

type Factory func() (ConnEle,error)

type Conn struct {
	conn ConnEle
	time time.Time
}

type ConnPool struct {
	mu sync.Mutex
	conns chan *Conn
	factory Factory
	closed bool
	connTimeOut time.Duration
}

func NewConnPool(factory Factory,capacity int,connTimeOut time.Duration) (*ConnPool,error){
	cp := &ConnPool{
		mu:          sync.Mutex{},
		conns:       make(chan *Conn,capacity),
		factory:     factory,
		closed:      false,
		connTimeOut: connTimeOut,
	}
	for i:=0; i<capacity; i++{
		connRes, err:=cp.factory()
		if err != nil {
			cp.Close()
			log.Debugf("tcp connection pool factory error")
		}
		cp.conns <- &Conn{
			conn: connRes,
			time: time.Now(),
		}
	}

	return cp,nil
}

func (cp *ConnPool) Get() (ConnEle, error){
	for{
		select {
		case connRes,ok := <-cp.conns:
			if !ok {
				return nil,errors.New("tcp connection pool closed")
			}
			if time.Now().Sub(connRes.time) > cp.connTimeOut {
				connRes.conn.Close()
				continue
			}
			log.Debugf("get an conn")
			return connRes.conn,nil
		default:
			connRes,err := cp.factory()
			if err != nil {
				return nil, err
			}
			log.Debugf("create an conn")
			return connRes,nil
		}
	}
}

func (cp *ConnPool) Put(conn ConnEle) error {
	if cp.closed {
		return errors.New("tcp connection pool closed")
	}
	select {
	case cp.conns <- &Conn{conn: conn,time: time.Now()}:
		return nil
	default:
		conn.Close()
		return errors.New("tcp connection pool closed")
	}
}

func (cp *ConnPool) Close(){
	if cp.closed {
		return
	}
	cp.mu.Lock()
	cp.closed=true
	close(cp.conns)
	for conn := range cp.conns {
		conn.conn.Close()
	}
	cp.mu.Unlock()
}

func (cp *ConnPool) len() int{
	return len(cp.conns)
}
