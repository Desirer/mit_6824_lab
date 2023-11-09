package raft

import "time"

func (rf *Raft) passHeartBeatTime() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.heart_beat_start_time) > rf.heart_beat_interval
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.election_start_time = time.Now() //重新设置选举定时器
	Debug(dTimer, "S%d receive AE from S%d", rf.me, args.LeaderId)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.heart_beat_start_time = time.Now() //重新设置心跳定时器
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			reply := AppendEntriesReply{}
			Debug(dLeader, "S%d send HeartMsg to S%d", args.LeaderId, index)
			ok := rf.sendAppendEntries(index, &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}
