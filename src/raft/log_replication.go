package raft

import (
	"sort"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry //empty for heartbeat msg
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int //不匹配时，返回不匹配的log任期号，不存在返回-1
	Xindex  int //不匹配的任期的第一条log所在index
	Xlen    int //Xterm为-1时，log的长度
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d send HeartMsg to S%d", args.LeaderId, server)
	} else {
		Debug(dLeader, "S%d send AEMsg to S%d", args.LeaderId, server)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d receive HeartMsg to S%d", rf.me, args.LeaderId)
	} else {
		Debug(dLeader, "S%d receive AEMsg to S%d", rf.me, args.LeaderId)
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		Debug(dLeader, "S%d deny AEMsg from S%d, cause of old term", rf.me, args.LeaderId)
		return
	}
	rf.becomeFollower(args.Term) //收到current leader的消息就应该重新设置选举定时器
	// 0、最后一条log的index比preLogIndex小
	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Xterm = -1
		reply.Xlen = len(rf.log)
		Debug(dLeader, "S%d deny AEMsg from S%d, cause of short log", rf.me, args.LeaderId)
		return
	}
	// 1、最后一条log的index大于等于preLogIndex
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Xterm = rf.log[args.PrevLogIndex].Term
		// 找到Xterm任期的第一条log位置
		pos := args.PrevLogIndex - 1
		for pos >= 1 && rf.log[pos].Term == reply.Xterm {
			pos--
		}
		reply.Xindex = pos + 1
		Debug(dLeader, "S%d deny AEMsg from S%d, cause of mismatch log at PrevLogIndex", rf.me, args.LeaderId)
		return
	}
	// 此时PrevLogIndex位置的log已经匹配，需要检查1+PrevLogIndex的位置log是否匹配
	if len(args.Entries) > 0 && len(rf.log) > 1+args.PrevLogIndex &&
		rf.log[1+args.PrevLogIndex].Term != args.Entries[0].Term {
		// 不匹配直接删除
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	// append or overwrite Enries from leader
	for offset, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + offset
		if len(rf.log)-1 >= index {
			rf.log[index] = entry
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInteger(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d agree HeartMsg from S%d", rf.me, args.LeaderId)
	} else {
		Debug(dLeader, "S%d agree AEMsg from S%d", rf.me, args.LeaderId)
	}
	return
}

func (rf *Raft) logReplication() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendEntry(i)
	}
}
func (rf *Raft) sendEntry(targetServerId int) {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[targetServerId] - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      rf.log[rf.nextIndex[targetServerId]:],
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(targetServerId, &args, &reply)
	if !ok {
		if len(args.Entries) == 0 {
			Debug(dTimer, "S%d can't send HeartMsg to S%d", args.LeaderId, targetServerId)
		} else {
			Debug(dLeader, "S%d can't send AEMsg to S%d", args.LeaderId, targetServerId)
		}
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		Debug(dLog, "%v become follower ", rf.me)
	}
	// RPC发送前后状态一致性校验
	if rf.state != leader && args.Term != rf.currentTerm {
		if len(args.Entries) == 0 {
			Debug(dTimer, "S%d receive old HeartMsg reply from S%d", args.LeaderId, targetServerId)
		} else {
			Debug(dLeader, "S%d receive old AEMsg reply from S%d", args.LeaderId, targetServerId)
		}
		return
	}
	if !reply.Success {
		if reply.Xterm == -1 {
			rf.nextIndex[targetServerId] = reply.Xlen
		} else {
			// 往后退找到任期为XTerm的第一条日志
			pos := args.PrevLogIndex
			for pos >= 1 && rf.log[pos].Term > reply.Xterm {
				pos--
			}
			if rf.log[pos].Term != reply.Xterm {
				// 没有找到任期Xterm的日志
				rf.nextIndex[targetServerId] = reply.Xindex
			} else {
				// 找到了任期Xterm的日志
				rf.nextIndex[targetServerId] = pos + 1
			}
		}
		if len(args.Entries) == 0 {
			Debug(dTimer, "S%d receive disagree HeartMsg reply from S%d", args.LeaderId, targetServerId)
		} else {
			Debug(dLeader, "S%d receive disagree AEMsg reply from S%d", args.LeaderId, targetServerId)
		}
		return
	}
	// reply == success
	rf.nextIndex[targetServerId] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[targetServerId] = args.PrevLogIndex + len(args.Entries)
	// leader 只能提交本任期内的日志
	majorityIndex := getMajoritySameIndex(rf.matchIndex)
	if rf.log[majorityIndex].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
		rf.commitIndex = majorityIndex
		rf.applyCond.Broadcast()
	}
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d receive agree HeartMsg reply from S%d", args.LeaderId, targetServerId)
	} else {
		Debug(dLeader, "S%d receive agree AEMsg reply from S%d", args.LeaderId, targetServerId)
	}
	return
}

func getMajoritySameIndex(matchIndex []int) int {
	tmp := make([]int, len(matchIndex))
	copy(tmp, matchIndex)

	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))

	idx := len(tmp) / 2
	return tmp[idx]
}
