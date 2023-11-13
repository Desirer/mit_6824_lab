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
		Debug(dTimer, "S%d receive HeartMsg from S%d", rf.me, args.LeaderId)
	} else {
		Debug(dLeader, "S%d receive AEMsg from S%d", rf.me, args.LeaderId)
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		Debug(dLeader, "S%d deny AEMsg from S%d, old term", rf.me, args.LeaderId)
		return
	}
	//rf.becomeFollower(args.Term)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if rf.state == candidate {
		rf.state = follower
		rf.persist()
	}

	rf.electionTimer.reset(getRandElectTimeout()) //收到current leader的消息就应该重新设置选举定时器

	if rf.log.LastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Xterm = -1
		reply.Xlen = rf.log.LastLogIndex + 1
		reply.Xindex = -1
		Debug(dLeader, "S%d deny AEMsg from S%d, short log, reply is %v", rf.me, args.LeaderId, reply)
		return
	}

	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Xterm = rf.log.get(args.PrevLogIndex).Term
		reply.Xindex = rf.snapLastLogIndex //兜底
		// 找到Xterm任期的第一条log位置(可能进行过快照）
		for idx := args.PrevLogIndex; idx >= rf.log.FirstLogIndex; idx-- {
			if rf.getLogTerm(idx-1) != reply.Xterm {
				reply.Xindex = idx
				break
			}
		}
		Debug(dLeader, "S%d deny AEMsg from S%d, mismatch log, reply %v", rf.me, args.LeaderId, reply)
		return
	}
	// append or overwrite Enries from leader
	for offset, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + offset
		if rf.log.FirstLogIndex <= idx && idx <= rf.log.LastLogIndex &&
			rf.log.get(idx).Term != entry.Term {
			rf.log.deleteAfter(idx)
		}
		if rf.log.FirstLogIndex <= idx && idx <= rf.log.LastLogIndex {
			rf.log.set(idx, entry)
		} else {
			rf.log.append(entry)
		}
	}

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		//index_of_last_new_entry := args.PrevLogIndex + len(args.Entries)
		index_of_last_new_entry := rf.log.LastLogIndex
		rf.commitIndex = minInteger(args.LeaderCommit, index_of_last_new_entry)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d agree HeartMsg from S%d", rf.me, args.LeaderId)
	} else {
		Debug(dLeader, "S%d agree AEMsg from S%d, log is %v", rf.me, args.LeaderId, rf.log)
		rf.persist() //减少persist的次数
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
func (rf *Raft) getLogTerm(index int) int {
	// index 可能会越过FirstLogIndex
	if rf.log.FirstLogIndex <= index && index <= rf.log.LastLogIndex {
		return rf.log.get(index).Term
	}
	if index == rf.snapLastLogIndex {
		return rf.snapLastLogTerm
	}
	Debug(dWarn, "S%v can't find Term for index%v", rf.me, index)
	return -1
}
func (rf *Raft) sendEntry(targetServerId int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[targetServerId] - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLogTerm(prevLogIndex),
		Entries:      rf.log.getAfter(prevLogIndex + 1),
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(targetServerId, &args, &reply)
	if !ok {
		if len(args.Entries) == 0 {
			Debug(dLog2, "S%d can't send HeartMsg to S%d", args.LeaderId, targetServerId)
		} else {
			Debug(dLog2, "S%d can't send AEMsg to S%d", args.LeaderId, targetServerId)
		}
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		Debug(dLog, "S%v become follower ", rf.me)
		return
	}
	// RPC发送前后状态一致性校验
	if rf.state != leader || args.Term != rf.currentTerm {
		if len(args.Entries) == 0 {
			Debug(dTimer, "S%d receive old HeartMsg reply from S%d", args.LeaderId, targetServerId)
		} else {
			Debug(dLeader, "S%d receive old AEMsg reply from S%d", args.LeaderId, targetServerId)
		}
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		next := match + 1
		rf.matchIndex[targetServerId] = maxInteger(rf.matchIndex[targetServerId], match)
		rf.nextIndex[targetServerId] = maxInteger(rf.nextIndex[targetServerId], next)

		// leader 只能提交本任期内的日志
		majorityIndex := getMajoritySameIndex(rf.matchIndex)
		if rf.getLogTerm(majorityIndex) == rf.currentTerm && majorityIndex > rf.commitIndex {
			rf.commitIndex = majorityIndex
			Debug(dCommit, "S%d receive agree reply from S%d, commitIndex %v", args.LeaderId, targetServerId, rf.commitIndex)
			rf.applyCond.Broadcast()
		}
		if len(args.Entries) == 0 {
			Debug(dTimer, "S%d receive agree HeartMsg reply from S%d", args.LeaderId, targetServerId)
		} else {
			Debug(dLeader, "S%d receive agree AEMsg reply from S%d", args.LeaderId, targetServerId)
		}
		return
	}
	// reply.Success == flase
	if reply.Xterm == -1 {
		rf.nextIndex[targetServerId] = reply.Xlen
	} else {
		// 往后退找到任期为XTerm的第一条日志（可能需要越过snapshot)
		pos := args.PrevLogIndex
		for pos >= rf.snapLastLogIndex && rf.log.get(pos).Term > reply.Xterm {
			pos--
		}
		if rf.log.get(pos).Term != reply.Xterm {
			// 没有找到任期Xterm的日志
			rf.nextIndex[targetServerId] = reply.Xindex
		} else {
			// 找到了任期Xterm的日志
			rf.nextIndex[targetServerId] = pos + 1
		}
	}
	// 判断是否越过了snapshot
	if rf.nextIndex[targetServerId] <= rf.snapLastLogIndex {
		rf.nextIndex[targetServerId] = rf.log.FirstLogIndex
		Debug(dSnap, "S%d receive too shot log from S%d", args.LeaderId, targetServerId)
		go rf.sendSnapshot(targetServerId)
	}
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d receive disagree HeartMsg reply from S%d", args.LeaderId, targetServerId)
	} else {
		Debug(dLeader, "S%d receive disagree AEMsg reply from S%d", args.LeaderId, targetServerId)
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
