package shardkv

import "6.5840/shardctrler"

type Op struct {
	Operation string // get/put/append/config/shard
	ShardIdx  int
	Key       string
	Value     string
	ClientIdx int64
	SeqNum    int64
	Cfg       shardctrler.Config
	Largs     LoadShardArgs
}

type UniversalReply struct {
	Err   Err
	Value string // for get operation
}

const (
	GET         = "GET"
	PUT         = "PUT"
	APPEND      = "APPEND"
	CFG_CHANGE  = "CONFIG_CHANGE"
	SHD_LOAD    = "SHARD_LOAD"
	SHD_DISCARD = "SHARD_DISCARD"
)

func (kv *ShardKV) handleApplyLoop() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			var reply UniversalReply
			need_reply := true
			switch op.Operation {
			case GET:
				{
					if kv.isResponsible(op.ShardIdx) {
						if tmpValue, ok := kv.kvStore[op.ShardIdx][op.Key]; ok {
							reply.Err = OK
							reply.Value = tmpValue
						} else {
							reply.Err = ErrNoKey
							reply.Value = ""
							//DPrintf("G%v S%v, read shard%v kvStore%v ", kv.gid, kv.me, op.ShardIdx, kv.kvStore[op.ShardIdx])
						}
						if kv.clientMap[op.ShardIdx][op.ClientIdx] < op.SeqNum {
							kv.clientMap[op.ShardIdx][op.ClientIdx] = op.SeqNum
						}
					} else {
						reply.Err = ErrWrongGroup
					}
				}
			case PUT:
				{
					if kv.isResponsible(op.ShardIdx) {
						if !kv.isDuplicated(op.ShardIdx, op.ClientIdx, op.SeqNum) {
							kv.clientMap[op.ShardIdx][op.ClientIdx] = op.SeqNum
							kv.kvStore[op.ShardIdx][op.Key] = op.Value
						}
						//DPrintf("G%v S%v,put update shard%v kvStore%v ", kv.gid, kv.me, op.ShardIdx, kv.kvStore[op.ShardIdx])
						reply.Err = OK
					} else {
						reply.Err = ErrWrongGroup
					}
				}
			case APPEND:
				{
					if kv.isResponsible(op.ShardIdx) {
						if !kv.isDuplicated(op.ShardIdx, op.ClientIdx, op.SeqNum) {
							kv.clientMap[op.ShardIdx][op.ClientIdx] = op.SeqNum
							kv.kvStore[op.ShardIdx][op.Key] += op.Value
						}
						reply.Err = OK
					} else {
						reply.Err = ErrWrongGroup
					}
				}
			case CFG_CHANGE:
				{
					kv.applyConfig(&op)
					need_reply = false
				}
			case SHD_LOAD:
				{
					kv.applyLoadShard(&op.Largs, &reply)
				}
			case SHD_DISCARD:
				{
					kv.applyDiscardShard(&op.Largs)
					need_reply = false
				}
			}
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && need_reply && currentTerm == msg.CommandTerm {
				ch := kv.getNotifyCh(msg.CommandIndex)
				ch <- reply
			}
			if kv.ifNeedSnapshot() {
				kv.makeSnapshot()
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.loadSnapshot(msg.SnapshotIndex, msg.Snapshot)
		}
	}
}

func (kv *ShardKV) applyConfig(op *Op) {
	if op.Cfg.Num != (1 + kv.curCfg.Num) {
		// 只能连续应用配置
		return
	}
	if !kv.shards_all_ok() {
		// 所有分片都就绪才能应用
		return
	}
	kv.preCfg = kv.curCfg
	kv.curCfg = op.Cfg
	for i, _ := range kv.curCfg.Shards {
		// 得到分片
		if kv.curCfg.Shards[i] == kv.gid && kv.preCfg.Shards[i] != kv.gid {
			if kv.preCfg.Shards[i] == 0 { // init
				kv.shardState[i] = Serv
			} else {
				kv.shardState[i] = Wait
			}
		}
		// 失去分片，马上将分片推送到新的拥有者
		if kv.curCfg.Shards[i] != kv.gid && kv.preCfg.Shards[i] == kv.gid {
			kv.shardState[i] = Push
		}
	}
	DPrintf("G%v S%v apply Cfg V%v %v, curV%v shardState %v", kv.gid, kv.me, op.Cfg.Num, op.Cfg.Shards, kv.curCfg.Num, kv.shardState)
}

// load shard from Wait -> Serv
func (kv *ShardKV) applyLoadShard(args *LoadShardArgs, reply *UniversalReply) {
	if args.Num < kv.curCfg.Num {
		reply.Err = OK
		return
	}
	if args.Num > kv.curCfg.Num {
		reply.Err = ErrWait
		return
	}
	if kv.shardState[args.ShardIdx] != Wait {
		reply.Err = OK
		return
	}
	kv.kvStore[args.ShardIdx] = DeepCopyString(args.ShardData)
	kv.clientMap[args.ShardIdx] = DeepCopyInt64(args.ClientMap)
	kv.shardState[args.ShardIdx] = Serv
	reply.Err = OK
	DPrintf("G%v S%v load V[%v]Shard%v, reply%v, shardState %v", kv.gid, kv.me, args.Num, args.ShardIdx, reply, kv.shardState)
	return
}

// discard shard from Push -> Discard
func (kv *ShardKV) applyDiscardShard(args *LoadShardArgs) {
	// 防止误删多次
	if _, ok := kv.shardState[args.ShardIdx]; ok && kv.shardState[args.ShardIdx] == Push {
		kv.shardState[args.ShardIdx] = Discard
		// garbage collection
		delete(kv.kvStore, args.ShardIdx)
		delete(kv.clientMap, args.ShardIdx)
		delete(kv.shardState, args.ShardIdx)
		DPrintf("G%v S%v discard V[%v]Shard%v, shardState %v", kv.gid, kv.me, args.Num, args.ShardIdx, kv.shardState)
	}
}
