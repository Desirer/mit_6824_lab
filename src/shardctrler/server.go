package shardctrler

import (
	"6.5840/raft"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const TIMEOUT_DURATION = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs     []Config // indexed by config num
	clientMap   map[int64]int64
	notifyChMap map[int]chan UniversalReply
	lastApplied int
}

type Op struct {
	// Your data here.
	Type  string
	Jargs JoinArgs
	Largs LeaveArgs
	Margs MoveArgs
	Qargs QueryArgs
}
type UniversalReply struct {
	Jreply JoinReply
	Lreply LeaveReply
	Mreply MoveReply
	Qreply QueryReply
}

func (sc *ShardCtrler) getNotifyCh(index int) chan UniversalReply {
	ch, ok := sc.notifyChMap[index]
	if !ok {
		sc.notifyChMap[index] = make(chan UniversalReply, 1)
		ch = sc.notifyChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) ifDuplicated(clinetId int64, seqNum int64) bool {
	lastSeqNum, ok := sc.clientMap[clinetId]
	if !ok {
		return false
	}
	return seqNum <= lastSeqNum
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	defer DPrintf("SC%v reply Join from C[%v][%v] with %v", sc.me, args.ClientId, args.SeqNum, reply)
	sc.mu.Lock()
	op := Op{Type: "Join", Jargs: *args}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChMap, index) //删除键值对，由垃圾回收算法释放管道
		sc.mu.Unlock()
	}()
	select {
	case ureply := <-ch:
		{
			reply.Err = ureply.Qreply.Err
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		{
			reply.Err = TIMEOUT
			return
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	defer DPrintf("SC%v reply Leave from C[%v][%v] with %v", sc.me, args.ClientId, args.SeqNum, reply)
	sc.mu.Lock()
	op := Op{Type: "Leave", Largs: *args}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	//reply.WrongLeader = false
	reply.WrongLeader = false
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChMap, index) //删除键值对，由垃圾回收算法释放管道
		sc.mu.Unlock()
	}()
	select {
	case ureply := <-ch:
		{
			reply.Err = ureply.Qreply.Err
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		{
			reply.Err = TIMEOUT
			return
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	defer DPrintf("SC%v reply Move from C[%v][%v] with %v", sc.me, args.ClientId, args.SeqNum, reply)
	sc.mu.Lock()
	op := Op{Type: "Move", Margs: *args}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChMap, index) //删除键值对，由垃圾回收算法释放管道
		sc.mu.Unlock()
	}()
	select {
	case ureply := <-ch:
		{
			reply.Err = ureply.Qreply.Err
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		{
			reply.Err = TIMEOUT
			return
		}
	}
}

// query只读请求不作重复过滤
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	defer DPrintf("SC%v reply Query%v C[%v][%v] with %v", sc.me, args.Num, args.ClientId, args.SeqNum, reply)
	sc.mu.Lock()
	op := Op{Type: "Query", Qargs: *args}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChMap, index) //删除键值对，由垃圾回收算法释放管道
		sc.mu.Unlock()
	}()
	select {
	case ureply := <-ch:
		{
			reply.Config = ureply.Qreply.Config
			reply.Err = ureply.Qreply.Err
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		{
			reply.Err = TIMEOUT
			return
		}
	}
}

func (sc *ShardCtrler) handleApplyLoop() {
	for {
		msg := <-sc.applyCh
		//DPrintf("SC%v receive msg %v", sc.me, msg)
		sc.mu.Lock()
		if msg.CommandIndex <= sc.lastApplied {
			sc.mu.Unlock()
			continue
		}
		sc.lastApplied = msg.CommandIndex
		op := msg.Command.(Op)
		var uReply UniversalReply
		switch op.Type {
		case "Query":
			{
				var reply QueryReply
				sc.handleQuery(&op.Qargs, &reply)
				uReply.Qreply = reply
			}
		case "Move":
			{
				var reply MoveReply
				sc.handleMove(&op.Margs, &reply)
				uReply.Mreply = reply
			}
		case "Leave":
			{
				var reply LeaveReply
				sc.handleLeave(&op.Largs, &reply)
				uReply.Lreply = reply
			}
		case "Join":
			{
				var reply JoinReply
				sc.handleJoin(&op.Jargs, &reply)
				uReply.Jreply = reply
			}
		}
		// only leader can reply client request
		if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
			ch := sc.getNotifyCh(msg.CommandIndex)
			ch <- uReply
		}
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientMap = make(map[int64]int64)
	sc.notifyChMap = make(map[int]chan UniversalReply)
	sc.lastApplied = 0
	go sc.handleApplyLoop()

	return sc
}
