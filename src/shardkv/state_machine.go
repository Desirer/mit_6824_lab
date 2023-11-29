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
	Largs     loadShardArgs
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
			switch op.Operation {
			case GET:
				{
					if tmpValue, ok := kv.kvStore[op.ShardIdx][op.Key]; ok {
						reply.Err = OK
						reply.Value = tmpValue
					} else {
						reply.Err = ErrNoKey
					}
					kv.clientMap[op.ShardIdx][op.ClientIdx] = op.SeqNum
				}
			case PUT:
				{
					if kv.clientMap[op.ShardIdx][op.ClientIdx] < op.SeqNum {
						kv.clientMap[op.ShardIdx][op.ClientIdx] = op.SeqNum
						kv.kvStore[op.ShardIdx][op.Key] = op.Value
					}
					reply.Err = OK
				}
			case APPEND:
				{
					if kv.clientMap[op.ShardIdx][op.ClientIdx] < op.SeqNum {
						kv.clientMap[op.ShardIdx][op.ClientIdx] = op.SeqNum
						kv.kvStore[op.ShardIdx][op.Key] += op.Value
					}
					reply.Err = OK
				}
			case CFG_CHANGE:
				{
					kv.applyConfig(&op)
					kv.mu.Unlock()
					continue
				}
			case SHD_LOAD:
				{
					kv.applyLoadShard(&op.Largs, &reply)
				}
			case SHD_DISCARD:
				{
					kv.applyDiscardShard(&op.Largs)
					kv.mu.Unlock()
					continue
				}
			}
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
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
	defer DPrintf("G%v S%v apply Cfg%v shardState %v", kv.gid, kv.me, op.Cfg.Shards, kv.shardState)
	if op.Cfg.Num < kv.curCfgNum {
		return
	}
	kv.nxtCfg = op.Cfg
	for i, _ := range kv.nxtCfg.Shards {
		// 得到分片
		if kv.nxtCfg.Shards[i] == kv.gid && kv.curCfg.Shards[i] != kv.gid {
			if kv.curCfg.Shards[i] == 0 { // init
				kv.shardState[i] = Serv
			} else {
				kv.shardState[i] = Wait
			}
		}
		// 失去分片，马上将分片推送到新的拥有者
		if kv.nxtCfg.Shards[i] != kv.gid && kv.curCfg.Shards[i] == kv.gid {
			kv.shardState[i] = Push
		}
	}
}

// load shard from Wait -> Serv
func (kv *ShardKV) applyLoadShard(args *loadShardArgs, reply *UniversalReply) {
	if args.Num < kv.curCfgNum {
		reply.Err = OK
		return
	}
	if args.Num > kv.curCfgNum {
		reply.Err = ErrWait
		return
	}
	if kv.shardState[args.ShardIdx] != Wait {
		reply.Err = OK
		return
	}
	kv.kvStore[args.ShardIdx] = args.ShardData
	kv.clientMap[args.ShardIdx] = args.ClientMap
	kv.shardState[args.ShardIdx] = Serv
	reply.Err = OK
	return
}

// discard shard from Push -> Discard
func (kv *ShardKV) applyDiscardShard(args *loadShardArgs) {
	kv.shardState[args.ShardIdx] = Discard
	// garbage collection
	delete(kv.kvStore, args.ShardIdx)
	delete(kv.clientMap, args.ShardIdx)
}
