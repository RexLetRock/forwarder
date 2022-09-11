package forwarder

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"

	"github.com/sirupsen/logrus"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type TcpClient struct {
	conn   net.Conn
	chans  chan []byte
	slice  []byte
	buffer []byte

	consumer chan []byte

	reader *io.PipeReader
	writer *io.PipeWriter
}

func NewTcpClient(addr string, topic string, consumer chan []byte) *TcpClient {
	s := &TcpClient{
		chans:    make(chan []byte, ChansSize),
		slice:    []byte{},
		buffer:   make([]byte, 1024*1000),
		consumer: consumer,
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logrus.Errorf("forwarder client connect err %v \n", err)
		return nil
	}
	s.conn = conn
	s.reader, s.writer = io.Pipe()

	// READ RESPONSE AND CALLBACK
	go func() {
		reader := bufio.NewReader(s.reader)
		for {
			msg, err := readWithEnd(reader)

			// DECODE
			data := MSG{}
			msgpack.Unmarshal(msg[4:len(msg)-3], &data)
			logrus.Errorf("CLIENT RECEIVE MSG %v %v \n", data, err)
			consumer <- data.B

			if err != nil {
				return
			}
		}
	}()

	// RECEIVE LOOP
	go func() {
		for {
			n, err := s.conn.Read(s.buffer)
			if err != nil {
				return
			}
			s.writer.Write(s.buffer[:n])
		}
	}()

	// WRITE LOOP
	go func() {
		cSend := 0
		for {
			msg := <-s.chans
			cSend += 1
			s.slice = append(s.slice, msg...)
			if cSend >= CSendSize {
				s.conn.Write(s.slice)
				s.slice = []byte{}
				cSend = 0
			}
		}
	}()

	// Send init topic event
	s.SendMessage(MSG{"topic", []byte(topic)})
	return s
}

func (s *TcpClient) Send(data []byte) {
	s.chans <- data
}

func (c *TcpClient) SendMessage(m interface{}) {
	b, _ := msgpack.Marshal(m)
	c.sendBinary(b)
}

func (c *TcpClient) ReplyToForwarder(data []byte, topic string) {
	msg := MSG{topic, data}
	b, _ := msgpack.Marshal(&msg)
	c.sendBinary(b)
}

func (c *TcpClient) sendBinary(m []byte) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(0))
	bend := append(bs, m...)
	bend = append(bend, []byte(ENDLINE)...)
	c.chans <- bend
}
