package kvraft

import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	GET    = 1
	PUT    = 2
	APPEND = 3
)

type Err string

type CommandRequest struct {
	Operation int // "get" or "put" or "append"
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
}

type CommandResponse struct {
	Err   Err
	Value string
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
