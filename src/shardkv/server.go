package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const TIMEOUT_DURATION = 500 * time.Millisecond
const QUERY_CONIFG_INTERVAL = 100 * time.Millisecond

const (
	Serv    = 1
	Push    = 2
	Wait    = 3
	Discard = 4
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk
	persister    *raft.Persister

	dead int32 // set by Kill()

	lastApplied int
	kvStore     map[int]map[string]string // shard id -> key value store
	clientMap   map[int]map[int64]int64   // shard id -> key value store
	notifyChMap map[int]chan UniversalReply
	leaderMap   map[int]int // gid -> leader id
	shardState  map[int]int //当前负责的分片状态：Serv、Push、Wait、Discard

	curCfg          shardctrler.Config //目前的配置
	nxtCfg          shardctrler.Config // 下一个配置
	curCfgNum       int                //目前的配置版本
	pushShardArgsCh chan loadShardArgs // 推送分片的参数通道
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isResponsible(shard int) bool {
	state, ok := kv.shardState[shard]
	if !ok {
		return false
	}
	return state == Serv
}

func (kv *ShardKV) isDuplicated(shardIdx int, clinetId int64, seqNum int64) bool {
	lastSeqNum, ok := kv.clientMap[shardIdx][clinetId]
	if !ok {
		return false
	}
	return seqNum <= lastSeqNum
}

func (kv *ShardKV) getNotifyCh(index int) chan UniversalReply {
	ch, ok := kv.notifyChMap[index]
	if !ok {
		kv.notifyChMap[index] = make(chan UniversalReply, 1)
		ch = kv.notifyChMap[index]
	}
	return ch
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	defer DPrintf("G%v S%v reply read C[%v][%v] Shard%v, %v", kv.gid, kv.me, args.ClientId, args.SeqNum, shard, reply)
	kv.mu.Lock()
	if !kv.isResponsible(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	op := Op{ShardIdx: shard, Key: args.Key, ClientIdx: args.ClientId, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
	}()
	select {
	case ureply := <-ch:
		{
			reply.Err = ureply.Err
			reply.Value = ureply.Value
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		{
			reply.Err = ErrTimeout
			return
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	defer DPrintf("G%v S%v reply PutAppend C[%v][%v] Shard%v, %v", kv.gid, kv.me, args.ClientId, args.SeqNum, shard, reply)
	kv.mu.Lock()
	if !kv.isResponsible(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.isDuplicated(shard, args.ClientId, args.SeqNum) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{ShardIdx: shard, Key: args.Key, Value: args.Value, ClientIdx: args.ClientId, SeqNum: args.SeqNum}
	if args.Op == "Put" {
		op.Operation = PUT
	} else {
		op.Operation = APPEND
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
	}()
	select {
	case ureply := <-ch:
		{
			reply.Err = ureply.Err
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		{
			reply.Err = ErrTimeout
			return
		}
	}
}

func (kv *ShardKV) ifNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	now_size := kv.persister.RaftStateSize()

	return now_size+100 >= kv.maxraftstate
}

func (kv *ShardKV) makeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvStore)
	e.Encode(kv.clientMap)
	e.Encode(kv.lastApplied)
	snapshot := w.Bytes()
	// 持有锁，确保kv.lastApplied不会改变
	kv.rf.Snapshot(kv.lastApplied, snapshot)
}

func (kv *ShardKV) loadSnapshot(index int, snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if index <= kv.lastApplied {
		return
	}
	if d.Decode(&kv.kvStore) != nil || d.Decode(&kv.clientMap) != nil || d.Decode(&kv.lastApplied) != nil {
		panic("error in parsing snapshot")
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.mck = shardctrler.MakeClerk(kv.ctrlers) //to connect with shardctrler
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[int]map[string]string)
	kv.clientMap = make(map[int]map[int64]int64)
	kv.notifyChMap = make(map[int]chan UniversalReply)
	kv.leaderMap = make(map[int]int)
	kv.shardState = make(map[int]int)

	kv.curCfg = shardctrler.Config{} // all shards all given to 0
	kv.nxtCfg = shardctrler.Config{}
	kv.curCfgNum = 0
	kv.pushShardArgsCh = make(chan loadShardArgs, 10) // 10个分片，10个缓冲区

	go kv.handleApplyLoop()

	go kv.queryConfigLoop()
	go kv.tryPushShardLoop()
	go kv.pushShardLoop()
	return kv
}
