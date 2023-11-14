package raft

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapLastLogIndex {
		Debug(dSnap, "S%v drop old SS %v <= %v", rf.me, index, rf.snapLastLogIndex)
		return
	}
	// 截断log
	rf.snapLastLogIndex = index
	rf.snapLastLogTerm = rf.log.get(index).Term
	rf.log.deleteBefore(index)
	// persist
	rf.persister.Save(rf.getRaftState(), snapshot)
	Debug(dSnap, "S%v make SS,sLI %v len(SS) %v log %v", rf.me, rf.snapLastLogIndex, rf.persister.SnapshotSize(), rf.log)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	Debug(dSnap, "S%d send SS to S%d, LII %v", args.LeaderId, server, args.LastIncludedIndex)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d receive SS request fromS%d", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		Debug(dSnap, "S%d deny SS request fromS%d, old term", rf.me, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.electionTimer.reset(getRandElectTimeout())
	if rf.state == candidate {
		rf.state = follower
	}
	if args.LastIncludedIndex <= rf.snapLastLogIndex {
		reply.Term = rf.currentTerm
		Debug(dSnap, "S%d deny SS request fromS%d, old snapshot, lII%v <= sLLI%v", rf.me, args.LeaderId, args.LastIncludedIndex, rf.snapLastLogIndex)
		return
	}
	rf.snapLastLogTerm = args.LastIncludedTerm
	rf.snapLastLogIndex = args.LastIncludedIndex
	if args.LastIncludedIndex > rf.log.LastLogIndex {
		// 1、snapshot太超前，舍弃现有的所有log
		rf.log.clean(args.LastIncludedIndex)
		Debug(dSnap, "S%d accept SS fromS%d,discard origin log %v", rf.me, args.LeaderId, rf.log)
	} else {
		// 2、firstIncludeIndex <= args.LastInculdeIndex <= lastLogIndex
		rf.log.deleteBefore(args.LastIncludedIndex)
		Debug(dSnap, "S%d accept SS fromS%d,truncate log %v", rf.me, args.LeaderId, rf.log)
	}
	// 快照包含了越前的状态，直接应用
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = maxInteger(rf.commitIndex, args.LastIncludedIndex)
		rf.applySnapshot(args)
	}
	// 否则只需保存快照
	rf.persister.Save(rf.getRaftState(), args.Data)
	reply.Term = rf.currentTerm
	return
}
func (rf *Raft) applySnapshot(args *InstallSnapshotArgs) {
	applyMag := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	rf.applyBufferCh <- applyMag
	//Debug(dSnap, "S%d apply SS,len%v", rf.me, len(args.Data))
}

func (rf *Raft) sendSnapshot(targetServerId int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapLastLogIndex,
		LastIncludedTerm:  rf.snapLastLogTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(targetServerId, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		Debug(dSnap, "S%v can't send snapshot to S%v", rf.me, targetServerId)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		Debug(dLog, "S%v become follower ", rf.me)
		return
	}
	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}
	rf.nextIndex[targetServerId] = maxInteger(rf.nextIndex[targetServerId], args.LastIncludedIndex+1)
	rf.matchIndex[targetServerId] = maxInteger(rf.matchIndex[targetServerId], args.LastIncludedIndex)
	Debug(dSnap, "S%v receive SS Reply from S%v, matchIndex %v, nextIndex%v", rf.me, targetServerId, rf.matchIndex, rf.nextIndex)
	return
}
