package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applyBuffer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		for !(rf.lastApplied < rf.commitIndex) {
			rf.applyCond.Wait()
		}
		// 逐条提交命令,保证顺序
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.get(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.log.get(rf.lastApplied).Term,
			}
			rf.applyBufferCh <- applyMsg
			//Debug(dCommit, "S%v commit index%v command%v", rf.me, applyMsg.CommandIndex, applyMsg.Command)
		}
	}
}
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		msg := <-rf.applyBufferCh
		rf.applyCh <- msg
		if msg.CommandValid {
			Debug(dCommit, "S%v commit index%v command%v", rf.me, msg.CommandIndex, msg.Command)
		} else {
			Debug(dCommit, "S%v commit SS index%v len%v", rf.me, msg.SnapshotIndex, len(msg.Snapshot))
		}
	}
}
