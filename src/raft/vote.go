package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) passElectionTime() bool {
	return time.Since(rf.election_start_time) > rf.election_interval
}

func (rf *Raft) getRandElectTimeout() time.Duration {
	return time.Duration(ELECTION_BASE_TIME+rand.Int63()%ELECTION_BASE_TIME) * time.Millisecond
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d receive VoteAsk from S%d", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.election_start_time = time.Now() //重新设置选举定时器
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d  VoteGranted to S%d", rf.me, args.CandidateId)
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	Debug(dVote, "S%d  VoteDeny to S%d", rf.me, args.CandidateId)
	return
}

// 开启新一轮选举
func (rf *Raft) doElection() {
	rf.mu.Lock()
	Debug(dVote, "S%d start election", rf.me)
	rf.election_start_time = time.Now() //重新设置选举定时器
	rf.election_interval = rf.getRandElectTimeout()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.count = 1 //投一票给自己
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.askVote(i, &args)
	}
}
func (rf *Raft) askVote(targetServerId int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	Debug(dVote, "S%d send requestVote to S%d", rf.me, targetServerId)
	ok := rf.sendRequestVote(targetServerId, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}
	if rf.state != candidate {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.Term != rf.currentTerm {
		return
	}
	if reply.VoteGranted {
		rf.count++
		Debug(dVote, "S%d receive VoteGranted from S%d", rf.me, targetServerId)
		if 2*rf.count > len(rf.peers) {
			rf.becomeLeader()
		}
	} else {
		Debug(dVote, "S%d receive VoteDeny from S%d", rf.me, targetServerId)
		return
	}
}
