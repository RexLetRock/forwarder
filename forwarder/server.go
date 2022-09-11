package forwarder

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const MaxClientTopic = 1000

var ConnHandleMap = NewConcurrentMap()

func ServerStart(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Start forwarder server err %v \n", err)
	}
	go ServerListen(listener)
	time.Sleep(TimeToDelay * time.Second)
}

func ServerListen(listener net.Listener) {
	defer listener.Close()
	for {
		if conn, err := listener.Accept(); err == nil {
			go handleConn(conn)
		}
	}
}

func handleConn(conn net.Conn) {
	handle := ConnHandleCreate(conn)
	handle.Handle()
}

type ConnHandle struct {
	readerReq *io.PipeReader
	writerReq *io.PipeWriter

	readerRes *io.PipeReader
	writerRes *io.PipeWriter

	buffer []byte
	// slice  []byte
	conn net.Conn
}

func ConnHandleCreate(conn net.Conn) *ConnHandle {
	p := &ConnHandle{
		buffer: make([]byte, 1024*1000),
		conn:   conn,
	}
	p.readerReq, p.writerReq = io.Pipe()
	p.readerRes, p.writerRes = io.Pipe()

	// Request flow
	go func() {
		reader := bufio.NewReader(p.readerReq)
		for {
			msg, _ := readWithEnd(reader)

			// DECODE
			data := MSG{}
			if err := msgpack.Unmarshal(msg[4:len(msg)-3], &data); err != nil {
				logrus.Errorf("MSG %v %v \n", data, err)
			}

			switch data.A {
			case "topic":
				logrus.Errorf("Server add new topic %v \n", string(data.B))
				ConnHandleMap.Set(string(data.B), p)
			default:
				pConn, _ := ConnHandleMap.Get(string(data.A))
				pConn.(*ConnHandle).conn.Write(msg)
			}
		}
	}()

	// Response flow
	return p
}

func (s *ConnHandle) Handle() error {
	defer s.conn.Close()
	for {
		n, err := s.conn.Read(s.buffer)
		if err != nil {
			return err
		}
		s.writerReq.Write(s.buffer[:n])
	}
}
