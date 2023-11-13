package raft

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       int
	currentTerm int
	votedFor    int
	//log         []Entry       //存储每条log
	log       *Log
	applyCh   chan ApplyMsg //用于提交entry
	applyCond *sync.Cond    // 条件变量，用于applyEntry协程

	// volatile on each server
	commitIndex int //已经提交的最大 log entry 的index
	lastApplied int //向客户端回复的最大 log entry 的index

	// leader 独有
	nextIndex  []int // 要向每个server发送的下一条 log entry 的index
	matchIndex []int // 与每个server同步复制的entry的最大index

	// 辅助变量
	electionTimer Timer
	pingTimer     Timer
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

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	Debug(dPersist, "S%v persist", rf.me)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		Debug(dWarn, "S%v read blank state", rf.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("decode wrong!")
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.state
	index := rf.log.LastLogIndex + 1
	term := rf.currentTerm
	if state != leader {
		return index, term, false
	}

	rf.log.append(Entry{term, command})
	rf.matchIndex[rf.me] = rf.log.LastLogIndex
	Debug(dCommit, "S%v append a command %v, log is %v", rf.me, command, rf.log)
	rf.persist()

	go rf.logReplication()
	return index, term, true
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
			if rf.electionTimer.passTime() {
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
				rf.electionTimer.reset(getRandElectTimeout())
				rf.doElection()
			}
		case candidate:
			if rf.electionTimer.passTime() {
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
				rf.electionTimer.reset(getRandElectTimeout())
				rf.doElection()
			}
		case leader:
			if rf.pingTimer.passTime() {
				rf.pingTimer.reset(HEART_BEAT_INTERVAL)
				rf.logReplication()
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
	rf.electionTimer = Timer{}
	rf.pingTimer = Timer{}
	rf.log = &Log{
		Entries:       make([]Entry, 0),
		FirstLogIndex: 1,
		LastLogIndex:  0,
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()
	if rf.state == leader {
		rf.becomeLeader()
	}
	Debug(dPersist, "S%v recover, log is %v", rf.me, rf.log)
	rf.mu.Unlock()

	rf.electionTimer.reset(getRandElectTimeout())
	rf.pingTimer.reset(HEART_BEAT_INTERVAL)
	go rf.ticker()
	go rf.applyLoop()

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
	//Debug(dLog, "S%d become candidate", rf.me)
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.persist()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for j, _ := range rf.peers {
		rf.nextIndex[j] = rf.log.LastLogIndex + 1
	}
	//go rf.logReplication()
	Debug(dLog, "S%d become leader,log is %v", rf.me, rf.log)
}

func (rf *Raft) info() string {
	return fmt.Sprintf("S%vT%v", rf.me, rf.currentTerm)
}
