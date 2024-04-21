// Package raft implements the Raft distributed consensus algorithm.
package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// State represents the different states of a Raft node.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Mutex to protect access to shared state
	peers     []*labrpc.ClientEnd // Array of RPC client objects to communicate with other nodes
	persister *Persister          // Persister object to store Raft state persistently
	me        int                 // Index of the current node in the peers array
	dead      int32               // Indicates whether the node is dead (1) or alive (0)

	state       State      // The current role of the Raft node (Follower, Candidate, or Leader).
	currentTerm int        // The current term the Raft node is in.
	votedFor    int        // The candidate ID that the Raft node voted for in the current term.
	log         []LogEntry // Log entries maintained by the Raft node.
	commitIndex int        // The highest log entry known to be committed.
	lastApplied int        // The highest log entry applied to the state machine.
	nextIndex   []int      // For each server, the index of the next log entry to send to that server (Leader only).
	matchIndex  []int      // For each server, the index of the highest log entry known to be replicated on that server (Leader only).
	Leader      int

	lastIndex int // Index of the last log entry in the previous snapshot
	lastTerm  int // Term of the last log entry in the previous snapshot

	electionTimeout  time.Time // The time at which the current node's election timeout expires
	heartbeatTimeout time.Time // The time at which the current node's heartbeat timeout expires

	applyCh chan ApplyMsg // Channel used to apply committed log entries to the state machine
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// Start is used to start a new operation on the Raft cluster.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == Leader

	if isLeader {
		logEntry := LogEntry{
			LogTerm:  rf.currentTerm,
			LogIndex: rf.getLastIndex(),
			Command:  command,
		}
		rf.log = append(rf.log, logEntry)
	}

	index := rf.getLastIndex()
	term := rf.currentTerm
	rf.persist()

	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.

// GetState returns the current term and whether the current node is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	return term, rf.state == Leader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

// Ticker periodically checks the state of the Raft node and triggers elections or heartbeats.
func (rf *Raft) Ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		now := time.Now()
		timer := time.Duration(rand.Intn(500-300)+300) * time.Millisecond
		finished := now.Sub(rf.electionTimeout)
		if rf.state == Follower {
			if timer <= finished {
				rf.state = Candidate
			}
		}
		if rf.state == Candidate {
			rf.changeToLeader()
		}
	}
}

// Elect initiates the election process for the Raft node.
func (rf *Raft) Elect() {
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()

	votes := 1
	rf.votedFor = rf.me

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.currentTerm {
					if rf.state == Candidate {
						if reply.VoteGranted {
							votes += 1
							if votes > len(rf.peers)/2 {
								rf.state = Leader
								rf.nextIndex = make([]int, len(rf.peers))
								rf.matchIndex = make([]int, len(rf.peers))
								for i := 0; i < len(rf.peers); i++ {
									rf.nextIndex[i] = rf.getLastIndex() + 1
									rf.matchIndex[i] = 0
								}
								rf.heartbeatTimeout = time.Unix(0, 0)
							}
						}
					}
				} else if rf.currentTerm < reply.Term {
					rf.changeToFollower(reply.Term)
					rf.persist()
					return
				}
			}
		}(server)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// RequestVoteArgs is the argument structure for the RequestVote RPC.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the reply structure for the RequestVote RPC.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote is the RPC handler for processing incoming RequestVote RPCs.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.upToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.electionTimeout = time.Now()
		}
	}
	rf.persist()
}

// sendRequestVote sends a RequestVote RPC to the specified server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

// AppendEntriesArgs is the argument structure for the AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int
	PrevIndex    int
	PrevTerm     int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is a struct used for replying to the AppendEntries RPC call.
type AppendEntriesReply struct {
	Term     int
	Success  bool
	ConIndex int
	ConTerm  int
}

// AppendEntries processes the AppendEntries RPC call from a leader.
// It appends the given entries to its own log, updates the commit index,
// and sends a success or failure response.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If the term of the incoming RPC is less than the current term,
	// set the reply term to the current term and indicate failure.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If the term of the incoming RPC is greater than the current term,
	// change to follower state and update the term.
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
		rf.persist()
	}

	reply.Term = rf.currentTerm
	// Reset the election timeout as the leader is alive.
	rf.electionTimeout = time.Now()
	prevLogIndex := args.PrevIndex
	prevLogTerm := args.PrevTerm

	// Check if the log is consistent with the previous log entry.
	if prevLogIndex < rf.lastIndex {
		reply.ConIndex = 1
		return
	} else if prevLogIndex == rf.lastIndex {
		if prevLogTerm != rf.lastTerm {
			reply.ConIndex = 1
			return
		}
	} else {
		if prevLogIndex > rf.getLastIndex() {
			reply.ConIndex = rf.getLastIndex() + 1
			return
		}
		if rf.log[rf.getLogPosition(prevLogIndex)].LogTerm != prevLogTerm {
			reply.ConTerm = rf.log[rf.getLogPosition(prevLogIndex)].LogTerm
			for index := rf.lastIndex + 1; index <= prevLogIndex; index++ {
				if rf.log[rf.getLogPosition(index)].LogTerm == reply.ConTerm {
					reply.ConIndex = index
					break
				}
			}
			return
		}
	}

	// Append new entries to the log or update existing entries.
	for i, entry := range args.Entries {
		index := prevLogIndex + 1 + i
		logPosition := rf.getLogPosition(index)
		if index > rf.getLastIndex() {
			rf.log = append(rf.log, entry)
		} else {
			if rf.log[logPosition].LogTerm != entry.LogTerm {
				rf.log = rf.log[:logPosition]
				rf.log = append(rf.log, entry)
			}
		}
	}
	rf.persist()

	// Update the commit index if the leader's commit index is greater
	// than the current commit index.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.getLastIndex() < rf.commitIndex {
			rf.commitIndex = rf.getLastIndex()
		}
	}
	reply.Success = true
}

// Replicate is responsible for replicating log entries across the cluster.
func (rf *Raft) Replicate() {
	// Continuously run the replication process until the server is killed.
	for !rf.killed() {
		// Sleep for a short duration to avoid overloading the system.
		time.Sleep(10 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// If the server is not the leader, return.
			if rf.state != Leader {
				return
			}

			// Check if it's time to send a heartbeat.
			now := time.Now()
			if now.Sub(rf.heartbeatTimeout) < 100*time.Millisecond {
				return
			}
			rf.heartbeatTimeout = time.Now()

			// Iterate through all servers in the cluster.
			for server := 0; server < len(rf.peers); server++ {
				// Skip the current server.
				if server == rf.me {
					continue
				}

				// If nextIndex is smaller than or equal to lastIndex, update nextIndex and matchIndex.
				if rf.nextIndex[server] <= rf.lastIndex {
					rf.nextIndex[server] = rf.getLastIndex() + 1
					rf.matchIndex[server] = rf.lastIndex
					rf.updateCommitIndex()
				} else {
					// Prepare and send AppendEntries RPC to the other servers.
					func(server int) {
						args := AppendEntriesArgs{}
						args.Term = rf.currentTerm
						args.LeaderCommit = rf.commitIndex
						args.Entries = make([]LogEntry, 0)
						args.PrevIndex = rf.nextIndex[server] - 1

						if args.PrevIndex == rf.lastIndex {
							args.PrevTerm = rf.lastTerm
						} else {
							args.PrevTerm = rf.log[rf.getLogPosition(args.PrevIndex)].LogTerm
						}
						args.Entries = append(args.Entries, rf.log[rf.getLogPosition(args.PrevIndex+1):]...)
						go func() {
							reply := AppendEntriesReply{}
							if ok := rf.sendAppendEntries(server, &args, &reply); ok {
								rf.mu.Lock()
								defer rf.mu.Unlock()

								// Ensure the server is still in the same term.
								if rf.currentTerm != args.Term {
									return
								}
								// Update the server's state if it has a higher term.
								if reply.Term > rf.currentTerm {
									rf.state = Follower
									rf.currentTerm = reply.Term
									rf.votedFor = -1
									rf.persist()
									return
								}
								// Update nextIndex and matchIndex based on the success of AppendEntries RPC.
								if reply.Success {
									rf.nextIndex[server] = args.PrevIndex + len(args.Entries) + 1
									rf.matchIndex[server] = rf.nextIndex[server] - 1
									rf.updateCommitIndex()
								} else {
									if reply.ConTerm != -1 {
										conflictTermIndex := -1
										for index := args.PrevIndex; index > rf.lastIndex; index-- {
											if rf.log[rf.getLogPosition(index)].LogTerm == reply.ConTerm {
												conflictTermIndex = index
												break
											}
										}
										if conflictTermIndex != -1 {
											rf.nextIndex[server] = conflictTermIndex
										} else {
											rf.nextIndex[server] = reply.ConIndex
										}
									} else {
										rf.nextIndex[server] = reply.ConIndex
									}
								}
							}
						}()
					}(server)
				}
			}
		}()
	}
}

// ApplyMsg is a struct containing the information required to apply a command or snapshot to the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Commit is responsible for applying committed log entries to the state machine.
func (rf *Raft) Commit(applyCh chan ApplyMsg) {
	commitCondition := false
	// Continuously run the commit process until the server is killed.
	for !rf.killed() {
		if commitCondition {
			time.Sleep(10 * time.Millisecond)
		}
		rf.mu.Lock()
		commitCondition = true
		// Apply all unapplied committed log entries.
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.getLogPosition(rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[entry].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			applyCh <- applyMsg
			rf.mu.Lock()
			commitCondition = false
		}
		rf.mu.Unlock()
	}
}

// sendAppendEntries sends an AppendEntries RPC to the specified server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

// persist saves Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIndex)
	e.Encode(rf.lastTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIndex)
	d.Decode(&rf.lastTerm)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.

// CondInstallSnapshot checks whether to switch to a snapshot based on the lastIncludedTerm and lastIncludedIndex.

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if rf.commitIndex >= lastIncludedIndex {
	// 	return false
	// }

	// if rf.lastIndex() < lastIncludedIndex {
	// 	rf.log = rf.log[:1]
	// } else {

	// }
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// type InstallSnapshotArgs struct {
// 	Term      int
// 	Leader    int
// 	lastIndex int
// 	lastTerm  int
// 	Data      []byte
// }

// type InstallSnapshotReply struct {
// 	Term int
// }

// Snapshot trims Raft's log based on the specified index and saves the snapshot.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
}

// func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if args.Term < rf.currentTerm {
// 		return
// 	}

// 	if args.Term > rf.currentTerm {
// 		rf.changeToFollower(args.Term)
// 		rf.persist()
// 	}

// 	rf.electionTimeout = time.Now()
// 	rf.Leader = args.Leader

// 	if rf.lastIndex >= args.lastIndex {
// 		return
// 	}
// 	ApplyMsg := ApplyMsg{
// 		SnapshotValid: true,
// 		Snapshot:      args.Data,
// 		SnapshotTerm:  args.lastTerm,
// 		SnapshotIndex: args.lastIndex,
// 	}
// 	rf.applyCh <- ApplyMsg
// }

// ************* Helper Functions *************//

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastIndex() int {
	return rf.lastIndex + len(rf.log)
}

func (rf *Raft) getLastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastTerm
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].LogTerm
	}
	return
}

func (rf *Raft) getLogPosition(index int) (pos int) {
	return index - rf.lastIndex - 1
}

func (rf *Raft) changeToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) changeToLeader() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimeout = time.Now()
	rf.persist()
	rf.Elect()
	rf.mu.Unlock()
}

func (rf *Raft) upToDate(candidateIndex int, candidateTerm int) bool {
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()
	upToDate := candidateTerm > lastLogTerm || (candidateTerm == lastLogTerm && candidateIndex >= lastLogIndex)
	return upToDate
}

func (rf *Raft) updateCommitIndex() {
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.getLastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastIndex || rf.log[rf.getLogPosition(newCommitIndex)].LogTerm == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
}

//********************************************//

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

// Make function initializes a new Raft instance and starts necessary goroutines for the Raft protocol.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Initialization of the Raft instance is done here.
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.votedFor = -1
	rf.lastIndex = 0
	rf.lastTerm = 0
	rf.electionTimeout = time.Now()
	rf.applyCh = applyCh
	rf.lastApplied = rf.lastIndex

	var applyMsg = &ApplyMsg{}
	rf.applyCh <- *applyMsg

	rf.readPersist(persister.ReadRaftState())

	// Start the Ticker goroutine
	go rf.Ticker()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.Replicate()
	}

	go rf.Commit(applyCh)

	return rf
}
