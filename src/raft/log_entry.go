package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogEntries struct {
	logs        []LogEntry
	commitIndex int
	lastApplied int
	SnapShotEntry
}

func (logEntries *LogEntries) GetLogItem(i int) LogEntry {
	idx := i - logEntries.lastIncludedIndex - 1
	if idx < 0 {
		return LogEntry{logEntries.lastIncludedTerm, nil}
	}
	return logEntries.logs[idx]
}
func (logEntries *LogEntries) GetLogItems(start, end int) []LogEntry {
	return logEntries.logs[start-logEntries.lastIncludedIndex-1 : end-logEntries.lastIncludedIndex-1]
}
func (logEntries *LogEntries) SetLogItems(start int, entries []LogEntry) {
	if start-logEntries.lastIncludedIndex-1 <= 0 {
		logEntries.logs = entries
	} else {
		logEntries.logs = append(logEntries.logs[:start-logEntries.lastIncludedIndex-1], entries...)
	}
	logEntries.lastApplied = logEntries.lastIncludedIndex + len(logEntries.logs)
}

func (logEntries *LogEntries) MatchLogs(index, term int) bool {
	if index == logEntries.lastIncludedIndex {
		return term == logEntries.lastIncludedTerm
	} else if index < logEntries.lastIncludedIndex {
		return false
	}
	return index <= logEntries.lastApplied && logEntries.GetLogItem(index).Term == term
}
