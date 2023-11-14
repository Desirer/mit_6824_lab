package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	Debug(dSnap, "S%d send SS to S%d, LII %v len(SS) %v", args.LeaderId, server, args.LastIncludedIndex, len(args.Data))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d receive SS request fromS%d", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dSnap, "S%d deny SS request fromS%d, old term", rf.me, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.LastIncludedIndex <= rf.snapLastLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dSnap, "S%d deny SS request fromS%d, old snapshot", rf.me, args.LeaderId)
		return
	}
	// 收到有效镜像，全部重置StateMachine的状态(not good enough)
	rf.snapLastLogTerm = args.LastIncludedTerm
	rf.snapLastLogIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	//if rf.lastApplied < args.LastIncludedIndex {
	//	rf.lastApplied = args.LastIncludedIndex
	//}
	//if rf.commitIndex < args.LastIncludedIndex {
	//	rf.commitIndex = args.LastIncludedIndex
	//}
	// 1、snapshot太超前，舍弃现有的所有log
	if args.LastIncludedIndex >= rf.log.LastLogIndex {
		rf.log.clean()
		rf.log.FirstLogIndex = args.LastIncludedIndex + 1
		rf.log.LastLogIndex = args.LastIncludedIndex
		rf.persister.Save(rf.getRaftState(), args.Data)
		rf.applySnapshot(args)

		reply.Term = rf.currentTerm
		reply.Success = true
		Debug(dSnap, "S%d accept SS fromS%d,discard origin log %v", rf.me, args.LeaderId, rf.log)
		return
	}
	// 2、snapshot超过旧镜像，低于lastLogIndex, 截断部分log
	if rf.log.FirstLogIndex < args.LastIncludedIndex {
		rf.log.deleteBefore(args.LastIncludedIndex)
		rf.persister.Save(rf.getRaftState(), args.Data)
		rf.applySnapshot(args)

		reply.Term = rf.currentTerm
		reply.Success = true
		Debug(dSnap, "S%d accept SS fromS%d,truncate log %v", rf.me, args.LeaderId, rf.log)
		return
	}
}
func (rf *Raft) applySnapshot(args *InstallSnapshotArgs) {
	applyMag := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	rf.applyCh <- applyMag
	Debug(dSnap, "S%d apply SS,len%v", rf.me, len(args.Data))
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
	if !reply.Success {
		Debug(dSnap, "S%v receive deny SS Reply from S%v, matchIndex %v", rf.me, targetServerId, rf.matchIndex)
		return
	}
	rf.nextIndex[targetServerId] = maxInteger(rf.nextIndex[targetServerId], args.LastIncludedIndex+1)
	rf.matchIndex[targetServerId] = maxInteger(rf.matchIndex[targetServerId], args.LastIncludedIndex)
	Debug(dSnap, "S%v receive agree SS Reply from S%v, matchIndex %v", rf.me, targetServerId, rf.matchIndex)
	return
}
