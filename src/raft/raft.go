package raft

import (
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	follower  = 1
	candidate = 2
	leader    = 3
)
const FRESH_TIME = time.Duration(50) * time.Millisecond
const HEART_BEAT_INTERVAL = time.Duration(150) * time.Millisecond
const ELECTION_BASE_TIME = 400

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       int
	currentTerm int
	votedFor    int

	count                 int //投票数
	election_start_time   time.Time
	election_interval     time.Duration
	heart_beat_start_time time.Time
	heart_beat_interval   time.Duration
}

func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	if rf.state == leader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) persist() {}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case follower:
			if rf.passElectionTime() {
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
				rf.doElection()
			}
		case candidate:
			if rf.passElectionTime() {
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
				rf.doElection()
			}
		case leader:
			if rf.passHeartBeatTime() {
				rf.heartBeat()
			}
		}
		time.Sleep(FRESH_TIME)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heart_beat_interval = HEART_BEAT_INTERVAL
	rf.election_start_time = time.Now()             //设置选举定时器
	rf.election_interval = rf.getRandElectTimeout() //设置选举过时时间
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
}

func (rf *Raft) becomeCandidate() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	Debug(dLog, "S%d become candidate", rf.me)
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.election_interval = rf.getRandElectTimeout() //重新设置选举时间
	Debug(dLog, "S%d become follower", rf.me)
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.persist()
	go rf.heartBeat()
	Debug(dLog, "S%d become leader", rf.me)
}
