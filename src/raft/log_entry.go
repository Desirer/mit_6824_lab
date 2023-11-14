package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Entries       []Entry
	FirstLogIndex int //Entries[0]对应的真实index
	LastLogIndex  int //最后一条log对应的真实index
}

func (l *Log) clean(lastLogIndex int) {
	l.Entries = make([]Entry, 0)
	l.FirstLogIndex = lastLogIndex + 1
	l.LastLogIndex = lastLogIndex
}

func (l *Log) append(entry Entry) {
	l.Entries = append(l.Entries, entry)
	l.LastLogIndex = l.LastLogIndex + 1
}

func (l *Log) get(index int) Entry {
	//if index == 0 { //initial state for bootstrap
	//	return Entry{Term: -1, Command: -1}
	//}
	return l.Entries[index-l.FirstLogIndex]
}

func (l *Log) set(index int, entry Entry) {
	l.Entries[index-l.FirstLogIndex] = entry
}

func (l *Log) getAfter(index int) []Entry {
	// 得到index以及以后的全部entry
	realIndex := index - l.FirstLogIndex
	return l.Entries[realIndex:]
}

func (l *Log) deleteBefore(index int) {
	//将index及以下的log全部抛弃
	if index > l.LastLogIndex {
		Debug(dWarn, "truncate index %v out of range %v", index, l.LastLogIndex)
		return
	}
	realIndex := index - l.FirstLogIndex
	l.Entries = l.Entries[realIndex+1:]
	l.FirstLogIndex = index + 1
}
func (l *Log) deleteAfter(index int) {
	// 将index及以后的log全部删除
	realIndex := index - l.FirstLogIndex
	l.Entries = l.Entries[:realIndex]
	l.LastLogIndex = index - 1
}
