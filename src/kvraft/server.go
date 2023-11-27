package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const TIMEOUT_DURATION = 500 * time.Millisecond

type Op struct {
	Operation int // "get" or "put" or "append"
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int
	kvStore      map[string]string            // data store
	clientMap    map[int64]int64              // 维护每个client处理的最新命令
	channelMap   map[int]chan CommandResponse // for synchronize, waiting apply
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
	DPrintf("S[%v] receive req C[%v][%v], op %v, key%v, val%v ", kv.me, request.ClientId, request.CommandId, request.Operation, request.Key, request.Value)
	op := Op{Operation: request.Operation, ClientId: request.ClientId, CommandId: request.CommandId, Key: request.Key, Value: request.Value}
	kv.mu.Lock()
	if request.Operation != GET && kv.isDuplicate(&op) {
		response.Err = OK
		DPrintf("S[%v] reply C[%v][%v], duplicated", kv.me, request.ClientId, request.CommandId)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		response.Err = ErrWrongLeader
		DPrintf("S[%v] deny req C[%v][%v], wrong leader", kv.me, request.ClientId, request.CommandId)
		return
	}

	kv.mu.Lock()
	//DPrintf("S[%v] receive C[%v][%v], here2", kv.me, request.ClientId, request.CommandId)
	DPrintf("S[%v] receive C[%v][%v], here2, %v", kv.me, request.ClientId, request.CommandId, index)
	synCh := kv.getSynChannel(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.channelMap, index) //删除键值对，由垃圾回收算法释放管道
		kv.mu.Unlock()
	}()

	select {
	case result := <-synCh:
		DPrintf("S[%v] reply C[%v][%v], here5", kv.me, request.ClientId, request.CommandId)
		response.Err = result.Err
		response.Value = result.Value
		return
	case <-time.After(TIMEOUT_DURATION):
		response.Err = ErrTimeout
		DPrintf("S[%v] reply C[%v][%v], timeout", kv.me, request.ClientId, request.CommandId)
		return
	}
}
func (kv *KVServer) getSynChannel(key int) chan CommandResponse {
	ch, exist := kv.channelMap[key]
	if !exist {
		kv.channelMap[key] = make(chan CommandResponse, 1) // 1大小缓冲区十分重要
		ch = kv.channelMap[key]
	}
	return ch
}

func (kv *KVServer) isDuplicate(op *Op) bool {
	lastCommandId, ok := kv.clientMap[op.ClientId]
	if ok && op.CommandId <= lastCommandId {
		return true
	}
	return false
}
func (kv *KVServer) handelApplyLoop() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			if msg.CommandValid {
				op := msg.Command.(Op) // 类型断言，相当于类型转换
				var response CommandResponse
				DPrintf("S[%v] got C[%v][%v], msg", kv.me, op.ClientId, op.CommandId)
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf("S[%v] discard C[%v][%v], mag %v", kv.me, op.ClientId, op.CommandId, msg)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				DPrintf("S[%v] apply C[%v][%v], lastApplied %v", kv.me, op.ClientId, op.CommandId, kv.lastApplied)
				if op.Operation != GET && kv.isDuplicate(&op) {
					// 过滤 put 和 append 的重复请求
					response.Err = OK
				} else {
					switch op.Operation {
					case PUT:
						kv.kvStore[op.Key] = op.Value
					case APPEND:
						kv.kvStore[op.Key] += op.Value
						response.Err = OK
					case GET:
						if tmpValue, ok := kv.kvStore[op.Key]; ok {
							response.Err = OK
							response.Value = tmpValue
						} else {
							response.Err = ErrNoKey
						}
					}
					kv.clientMap[op.ClientId] = op.CommandId
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
					DPrintf("S[%v] send before C[%v][%v], here3, %v", kv.me, op.ClientId, op.CommandId, msg.CommandIndex)
					ch := kv.getSynChannel(msg.CommandIndex)
					ch <- response // 发送消息前先解锁
					DPrintf("S[%v] send after C[%v][%v], here4", kv.me, op.ClientId, op.CommandId)
				}
				if kv.ifNeedSnapshot() {
					kv.makeSnapshot(kv.lastApplied)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				// load snapshot
				kv.loadSnapshot(msg.SnapshotIndex, msg.Snapshot)
			} else {
				DPrintf("error!")
			}
		}
	}
}
func (kv *KVServer) ifNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	now_size := kv.persister.RaftStateSize()

	return now_size+100 >= kv.maxraftstate
}

func (kv *KVServer) makeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvStore)
	e.Encode(kv.clientMap)
	e.Encode(kv.lastApplied)
	snapshot := w.Bytes()

	kv.rf.Snapshot(index, snapshot)
}

func (kv *KVServer) loadSnapshot(index int, snapshot []byte) {
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

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.clientMap = make(map[int64]int64)
	kv.channelMap = make(map[int]chan CommandResponse)
	kv.lastApplied = 0
	// 崩溃时获取持久存储
	kv.loadSnapshot(kv.rf.GetLastIncludeIndex(), kv.persister.ReadSnapshot())
	go kv.handelApplyLoop()
	return kv
}
