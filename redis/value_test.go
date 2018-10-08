package redis

import (
	"bufio"
	"bytes"
	"testing"
)

func TestReply_ReadFrom(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("+PING\r\n")
	buf.WriteString("$4\r\nPONG\r\n")
	r := bufio.NewReader(buf)
	reply := BlankReply()
	v, err := reply.ReadFrom(r)
	if err != nil {
		t.Error(err)
	}
	if string(v.Bytes()) != "PING" {
		t.Errorf("Invalid value %s", v.Bytes())
		return
	}
	reply.Reset()
	v, err = reply.ReadFrom(r)
	if err != nil {
		t.Error(err)
	}
	if string(v.Bytes()) != "PONG" {
		t.Errorf("Invalid value %s", v.Bytes())
		return
	}
}
