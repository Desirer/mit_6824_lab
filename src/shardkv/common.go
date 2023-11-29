package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWait        = "ErrWait"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqNum   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqNum   int64
}

type GetReply struct {
	Err   Err
	Value string
}

type loadShardArgs struct {
	TargetGid int               // 目标组号
	Num       int               // shard所属config的版本号
	ShardIdx  int               // shard 索引
	ShardData map[string]string // shard数据
	ClientMap map[int64]int64   // 客户端历史
}

type loadShardReply struct {
	Err Err
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
