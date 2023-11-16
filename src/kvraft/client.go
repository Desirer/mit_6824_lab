package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int64
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand() //采用随机数生成id，有微小几率重复
	ck.commandId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	return ck.CommandExecutor(&CommandRequest{Operation: GET, Key: key})
}

func (ck *Clerk) Put(key string, value string) {
	ck.CommandExecutor(&CommandRequest{Operation: PUT, Key: key, Value: value})
}
func (ck *Clerk) Append(key string, value string) {
	ck.CommandExecutor(&CommandRequest{Operation: APPEND, Key: key, Value: value})
}
func (ck *Clerk) CommandExecutor(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		var response CommandResponse
		if !ck.servers[ck.leaderId].Call("KVServer.HandleRequest", request, &response) || response.Err == ErrWrongLeader || response.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++ // commandId不断递增
		return response.Value
	}
}
