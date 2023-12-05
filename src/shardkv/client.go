package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	seqNum    int64
	leaderMap map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqNum = 0
	ck.leaderMap = make(map[int]int)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			oldLeaderId := ck.leaderMap[gid] % len(servers)
			newLeaderId := oldLeaderId
			for {
				srv := ck.make_end(servers[newLeaderId])
				var reply GetReply
				DPrintf("C[%v][%v] get G[%v]S[%v] ", ck.clientId, ck.seqNum, gid, newLeaderId)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.seqNum++
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				ck.leaderMap[gid] = (ck.leaderMap[gid] + 1) % len(servers)
				newLeaderId = ck.leaderMap[gid]
				if newLeaderId == oldLeaderId {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			oldLeaderId := ck.leaderMap[gid] % len(servers)
			newLeaderId := oldLeaderId
			for {
				srv := ck.make_end(servers[newLeaderId])
				var reply GetReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.seqNum++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				ck.leaderMap[gid] = (ck.leaderMap[gid] + 1) % len(servers)
				newLeaderId = ck.leaderMap[gid]
				if newLeaderId == oldLeaderId {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
