package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Operation int // "get" or "put" or "append"
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
}
type IndexTerm struct {
	index int
	term  int
}

//	type Identify struct {
//		clientId int
//		commandId int
//	}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int                                // snapshot if log grows this big
	kvStore      map[string]string                  // data store
	clientMap    map[int64]int64                    // 维护每个client处理的最新命令
	channelMap   map[IndexTerm]chan CommandResponse // for synchronize, waiting apply
	//resultMap map[Identify] CommandResponse
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) HandleRequest(request *CommandRequest, response *CommandResponse) {
	if kv.killed() {
		response.Err = ErrWrongLeader
		return
	}
	_, leader := kv.rf.GetState()
	if !leader {
		response.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: request.Operation, ClientId: request.ClientId, CommandId: request.CommandId, Key: request.Key, Value: request.Value}
	index, term, _ := kv.rf.Start(op)
	synCh := kv.getSynChannel(IndexTerm{index: index, term: term})
	// todo 回收管道

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case result := <-synCh:
		response.Err = result.Err
		if result.Err == OK {
			response.Value = result.Value
			return
		}
	case <-timer.C:
		response.Err = ErrTimeout
		return
	}
}
func (kv *KVServer) getSynChannel(key IndexTerm) chan CommandResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.channelMap[key]
	if !exist {
		kv.channelMap[key] = make(chan CommandResponse)
		ch = kv.channelMap[key]
	}
	return ch
}

func (kv *KVServer) isDuplicate(op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commandId, ok := kv.clientMap[op.ClientId]
	if ok && op.CommandId < commandId {
		return true
	}
	return false
}
func (kv *KVServer) callBackApplyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		index := msg.CommandIndex
		term := msg.CommandTerm
		op := msg.Command.(Op) // 类型断言，相当于类型转换
		var response CommandResponse
		response.Err = OK
		if !kv.isDuplicate(&op) { //由状态机过滤重复的请求
			kv.mu.Lock()
			switch op.Operation {
			case PUT:
				kv.kvStore[op.Key] = op.Value
			case APPEND:
				kv.kvStore[op.Key] += op.Value
			case GET:
				tmpValue, ok := kv.kvStore[op.Key]
				if !ok {
					response.Err = ErrNoKey
				}
				response.Value = tmpValue
			}
			kv.clientMap[op.ClientId] = op.CommandId
			kv.mu.Unlock()
		}
		kv.getSynChannel(IndexTerm{index: index, term: term}) <- response //重复的请求直接返回ok
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.clientMap = make(map[int64]int64)
	kv.channelMap = make(map[IndexTerm]chan CommandResponse)

	go kv.callBackApplyLoop()
	return kv
}
