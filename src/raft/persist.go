package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	//snapShotByte := rf.GetSnapshotByte()
	rf.mu.Unlock()

	//log.Printf("Persist SERVER : %v Logs : %v, commit: %v", rf.me, logs, rf.commitIndex)
	//for i := 0; i < len(logs); i++ {
	//	e.Encode(logs[i])
	//}
	//log.Printf("logs length %v", len(rf.logs))

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//rf.persister.SaveStateAndSnapshot(data, snapShotByte)
	//log.Printf("save %v", data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	//rf.GetSnapshotEntry(rf.persister.ReadSnapshot()) // need reset state machine
	//log.Printf("Snap Entry : %v", rf.SnapShotEntry)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor int
	var logEntries []LogEntry
	//log.Printf("data %v %v", data, d.Decode(&currentTerm))
	if d.Decode(&currentTerm) == nil &&
		d.Decode(&voteFor) == nil &&
		d.Decode(&logEntries) == nil {
		rf.currentTerm, rf.voteFor, rf.logs = currentTerm, voteFor, logEntries
	} else {
		log.Fatal("Parse Error")
	}
	rf.lastApplied = rf.lastIncludedIndex + len(rf.logs)
}
