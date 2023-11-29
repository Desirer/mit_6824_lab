package shardkv

import (
	"time"
)

/*
以下全部由leader进行，包括询问配置，推送分片、装载分片，丢弃分片
*/

func (kv *ShardKV) all_ok() bool {
	for _, state := range kv.shardState {
		if state != Serv {
			return false
		}
	}
	kv.curCfg = kv.nxtCfg
	kv.curCfgNum = kv.curCfg.Num
	return true
}

// 询问配置
func (kv *ShardKV) queryConfigLoop() {
	for !kv.killed() {
		time.Sleep(QUERY_CONIFG_INTERVAL)
		kv.mu.Lock()
		// 只有leader才能询问配置
		_, isLeader := kv.rf.GetState()
		shard_ok := kv.all_ok()
		if !isLeader || !shard_ok {
			kv.mu.Unlock()
			continue
		}
		// 所有分片都OK才能进行下一次更新
		newCfg := kv.mck.Query(kv.curCfgNum + 1)
		// 询问到的配置版本号更大，下发配置
		if newCfg.Num == kv.curCfgNum+1 {
			DPrintf("G%v S%v got Cfg%v", kv.gid, kv.me, newCfg.Shards)
			kv.curCfgNum = newCfg.Num //先更新目前的配置号，避免Raft层的多次Start
			kv.nxtCfg = newCfg
			op := Op{Operation: CFG_CHANGE, Cfg: newCfg}
			kv.rf.Start(op)
		}
		kv.mu.Unlock()
	}
}

// 尝试推送（检测是否存在待发送的分片）
func (kv *ShardKV) tryPushShardLoop() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		// 只有leader才能进行 shard migration
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			continue
		}
		for i, state := range kv.shardState {
			if state == Push {
				// args 应该 deep copy
				args := loadShardArgs{
					TargetGid: kv.nxtCfg.Shards[i],
					Num:       kv.curCfgNum,
					ShardIdx:  i,
					ShardData: kv.kvStore[i],
					ClientMap: kv.clientMap[i],
				}
				kv.pushShardArgsCh <- args
			}
		}
		kv.mu.Unlock()
	}
}

// 推送分片（推送可能失败，状态未更新，会再次推送)
func (kv *ShardKV) pushShardLoop() {
	for args := range kv.pushShardArgsCh {
		servers, ok := kv.nxtCfg.Groups[args.TargetGid]
		if !ok {
			continue
		}
		for {
			sidx := kv.leaderMap[args.TargetGid] % len(servers)
			srv := kv.make_end(servers[sidx])
			var reply loadShardReply
			ok := srv.Call("ShardKV.loadShard", args, &reply)
			if ok {
				if reply.Err == OK {
					op := Op{Operation: SHD_DISCARD, Largs: args}
					kv.rf.Start(op) // Push完成丢弃分片
					break
				}
				if reply.Err == ErrWrongLeader {
					kv.leaderMap[args.TargetGid] = (kv.leaderMap[args.TargetGid] + 1) % len(servers)
					continue
				}
				if reply.Err == ErrTimeout {
					break // 服务端可能出现了某种错误，跳过这个分片
				}
			}
		}
	}
}

// 加载分片
func (kv *ShardKV) loadShard(args *loadShardArgs, reply *loadShardReply) {
	kv.mu.Lock()
	if args.Num != kv.curCfgNum {
		kv.mu.Unlock()
		return
	}
	op := Op{Operation: SHD_LOAD, Largs: *args}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
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
