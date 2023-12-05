package shardkv

import (
	"time"
)

/*
以下全部由leader进行，包括询问配置，推送分片、装载分片，丢弃分片
*/

func (kv *ShardKV) shards_all_ok() bool {
	for _, state := range kv.shardState {
		if state != Serv {
			return false
		}
	}
	return true
}

// 询问配置
func (kv *ShardKV) queryConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		// 只有leader才能询问配置
		_, isLeader := kv.rf.GetState()
		// 所有分片都OK才能进行下一次更新
		shard_ok := kv.shards_all_ok()
		if !isLeader || !shard_ok {
			kv.mu.Unlock()
			time.Sleep(QUERY_CONIFG_INTERVAL)
			continue
		}
		DPrintf("--G%v S%v ticker!%v", kv.gid, kv.me, kv.shardState)
		newCfg := kv.mck.Query(kv.curCfg.Num + 1)
		// 询问到的配置版本号更大，下发配置
		if newCfg.Num == kv.curCfg.Num+1 {
			DPrintf("G%v S%v got V%vCfg%v", kv.gid, kv.me, newCfg.Num, newCfg.Shards)
			op := Op{Operation: CFG_CHANGE, Cfg: newCfg}
			kv.rf.Start(op)
		}
		kv.mu.Unlock()
		time.Sleep(QUERY_CONIFG_INTERVAL)
	}
}

// 尝试推送（检测是否存在待发送的分片）
func (kv *ShardKV) tryPushShardLoop() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		// 不为leader，睡眠
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			continue
		}
		argsSlice := make([]LoadShardArgs, 0)
		for i, state := range kv.shardState {
			if state == Push {
				args := LoadShardArgs{
					TargetGid: kv.curCfg.Shards[i],
					Num:       kv.curCfg.Num,
					ShardIdx:  i,
					ShardData: DeepCopyString(kv.kvStore[i]),
					ClientMap: DeepCopyInt64(kv.clientMap[i]),
				}
				argsSlice = append(argsSlice, args)
			}
		}
		// 没有需要推送的分片，则睡眠
		if len(argsSlice) == 0 {
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()
		// 会阻塞的地方不加锁
		for _, arg := range argsSlice {
			kv.pushShardArgsCh <- arg
		}
	}
}

// 推送分片(直接向一个集群发送一轮推送，不负责失败重试）
func (kv *ShardKV) pushShardLoop() {
	for args := range kv.pushShardArgsCh {
		kv.mu.Lock()
		if servers, sok := kv.curCfg.Groups[args.TargetGid]; sok {
			// try each server to push shards one after another
			for i := 0; i < len(servers); i++ {
				srv := kv.make_end(servers[i])
				var reply LoadShardReply
				DPrintf("G%v S%v push V[%v]Shard[%v] to G%v S%v", kv.gid, kv.me, args.Num, args.ShardIdx, args.TargetGid, i)
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.LoadShard", &args, &reply)
				kv.mu.Lock()
				if ok {
					if reply.Err == OK && kv.shardState[args.ShardIdx] == Push { // Push完成丢弃分片
						op := Op{Operation: SHD_DISCARD, Largs: args}
						kv.rf.Start(op)
						break
					} else if reply.Err == ErrWrongLeader {
						continue
					}
				}
			}
		}
		kv.mu.Unlock()
	}
}

// 加载分片
func (kv *ShardKV) LoadShard(args *LoadShardArgs, reply *LoadShardReply) {
	//defer DPrintf("G%v S%v reply V[%v]Shard%v,reply%v,shardState %v", kv.gid, kv.me, args.Num, args.ShardIdx, reply, kv.shardState)
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if args.Num < kv.curCfg.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if args.Num > kv.curCfg.Num {
		reply.Err = ErrWait
		kv.mu.Unlock()
		return
	}
	op := Op{Operation: SHD_LOAD, Largs: *args}
	index, _, _ := kv.rf.Start(op)
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
