package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	Debug(dVote, "S%d send requestVote to S%d", args.CandidateId, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func up_to_date(term1 int, len1 int, term2 int, len2 int) bool {
	if term1 != term2 {
		return term1 > term2
	}
	return len1 > len2
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d receive VoteAsk from S%d", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d deny VoteAsk from S%d, old term", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if up_to_date(rf.log[len(rf.log)-1].Term, len(rf.log), args.LastLogTerm, 1+args.LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d deny VoteAsk from S%d, old log", rf.me, args.CandidateId)
		return
	}
	if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d deny VoteAsk from S%d, repeated VF ", rf.me, args.CandidateId)
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.electionTimer.reset(getRandElectTimeout()) // 同意投票后重置选举定时器
	Debug(dVote, "S%d VoteGranted to S%d", rf.me, args.CandidateId)
	return
}

// 开启新一轮选举
func (rf *Raft) doElection() {
	rf.mu.Lock()
	Debug(dVote, "S%d start election", rf.me)
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	//rf.count = 1 //投一票给自己
	count := 1
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.askVote(i, &args, &count)
	}
}
func (rf *Raft) askVote(targetServerId int, args *RequestVoteArgs, count *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(targetServerId, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		Debug(dVote, "S%d can't send vote to S%d", rf.me, targetServerId)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	// RPC前后状态一致性校验
	if rf.state != candidate && reply.Term != rf.currentTerm {
		Debug(dVote, "S%d receive old vote reply from S%d", rf.me, targetServerId)
		return
	}
	if !reply.VoteGranted {
		Debug(dVote, "S%d receive VoteDeny from S%d", rf.me, targetServerId)
		return
	}
	(*count)++
	Debug(dVote, "S%d receive VoteGranted from S%d", rf.me, targetServerId)
	if rf.state == candidate && 2*(*count) > len(rf.peers) {
		rf.becomeLeader()
	}
}
