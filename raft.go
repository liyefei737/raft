package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Term int
	Command interface {}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// node states
	curTerm int
	state   NodeState

	// log states
	logs        []Log
	commitIndex int //the highest log index known that is commited
	lastApplied int // the highest log index known that is applied
	applyCh     chan ApplyMsg
	newLogCh    chan bool
	//peer states
	nextIndexes  []int
	matchIndexes []int

	//election timeout states
	elecTimeoutBounds ElecTimeoutBounds
	randNumGen        *rand.Rand // random number generator that seed is initialized with time.Now().UnixNano()

	//candidate states
	votedFor        int // index for the candidate this nodeId voted for in its current Term, -1 indicates not voted yet
	heartbeatCh     chan HeartBeatMssg
	killedCh        chan bool
	stopHeartbeatCh chan bool
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.curTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.curTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	bytes := w.Bytes()
	rf.persister.SaveRaftState(bytes)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.curTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

type NodeState uint32

// an raft instance at any given time is in 1 of these 3 states
const (
	Follower = iota + 1
	Candidate
	Leader
)

func (n NodeState) String() string {
	switch n {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"

	default:
		return "Unknown"
	}
}

// in ms [min,max)
type ElecTimeoutBounds struct {
	// min is the minimum election timeout.
	// It should be at least several times of the time it takes for sending 1 heartbeat from the leader to a follower
	min int32
	// max affects how quickly the system can recover from the failed leader
	max int32
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidID     int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	VoteGranted bool
	Term        int

	// Your data here (2A).
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteChan chan int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	voteChan <- server
	return ok
}

//
// example AppendEntry RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	isCandidLogsMoreUpToDate := func() bool {
		// Raft determines which of two logs is more up-to-date
		// by comparing the index and Term of the last Entries in the
		// logs. If the logs have last Entries with different terms, then
		// the log with the later Term is more up-to-date. If the logs
		// end with the same Term, then whichever log is longer is
		// more up-to-date.
		if args.LastLogTerm > getLastLogTerm(rf) {
			return true
		} else if args.LastLogTerm == getLastLogTerm(rf) && args.LastLogIndex >= getLastLogIndex(rf) {
			return true
		}
		return false
	}()
	log.Printf("node %d(Term %d) requests node %d(Term %d) to vote candi more up to date? %v",args.CandidID, args.Term, rf.me, rf.curTerm, isCandidLogsMoreUpToDate)

	switch {
	case args.Term < rf.curTerm:
		reply.VoteGranted = false
		reply.Term = rf.curTerm
	// all cases bellow have argument's Term >= to rf.curTerm
	case args.Term > rf.curTerm:
		if isCandidLogsMoreUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidID
		} else {
			reply.VoteGranted = false
			rf.votedFor = -1
		}

		rf.curTerm = args.Term
		if rf.state == Leader {
			rf.stopHeartBeats()
		}
		rf.state = Follower
	case (rf.votedFor == -1 || rf.votedFor == args.CandidID) && isCandidLogsMoreUpToDate:
		//equal current term
		// this nodeId has either not voted or has already voted this candidate
		reply.VoteGranted = true
		rf.votedFor = args.CandidID
	default:
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		log.Printf("Request Vote no cases satisfied.\n")

	}

	log.Printf("node %d(Term %d) requests node %d(Term %d) to vote vote granted: %v\n ",args.CandidID, args.Term, rf.me, rf.curTerm, reply.VoteGranted)
	//log.Printf("Node %d asks nodeId %d to vote	Election Term : %d	Vote granted? %v", args.CandidID, rf.me, args.Term, reply.VoteGranted)
	//log.Printf("votedfor: %d | rf.Term | args.Term: %d | %d", rf.votedFor, rf.curTerm, args.Term)

}

// only a leader node can sedn appendEntry

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index for the log entry immediately preceeding the one to be appended
	PrevLogTerm  int // term of the log entry immediately preceeding the current one
	CommitIndex  int

	Entries []Log //log Entries to pass, empty for heartbeat
}

type AppendEntryReply struct {
	Term     int  // term for leader to update itself if the leader sess a term bigger than its own
	Accepted bool // return true if the receiving node matches the leader's previous log index and the term on it
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	// keep sending append try to a node until a response is received
	for {
		ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
		if ok {
			if reply.Term > rf.curTerm {
				rf.state = Follower
				rf.curTerm = reply.Term
				rf.votedFor = -1
				break
			}

			if len(args.Entries) == 0 {
				return
			}

			if reply.Accepted {
				rf.matchIndexes[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndexes[server] = rf.matchIndexes[server] + 1
				for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
					count := 1
					if rf.logs[N].Term == rf.curTerm {
						for i := range rf.peers {
							if rf.matchIndexes[i] >= N {
								count += 1
							}
						}
					}
					if count > len(rf.peers)/2 {
						fmt.Printf("Updating leader(%d) commitIndex from %d to %d\n",rf.me, rf.commitIndex, N)
						rf.commitIndex = N
						go rf.applyLogs()
					}
				}
				break
			} else {
				if rf.nextIndexes[server] == 0 {
					args.Entries = rf.logs
					continue
				}
				//fmt.Printf("Updating rf.nextIndexes[%d]: from %d to", server, rf.nextIndexes[server])
				rf.nextIndexes[server] -= 1
				args.Entries = rf.logs[rf.nextIndexes[server]:]
				//fmt.Printf(" %d\n", rf.nextIndexes[server])
			}

		} else {
			//log.Printf("Raft: [Id: %d | Term: %d | %v] - Communication error: AppendEntries() RPC failed", rf.me, rf.curTerm, rf.state)
			time.Sleep(15 * time.Millisecond)
		}
	}
}

//
// used to replicate log Entries in the leaders to its peers and heartbeat
//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// TODO add logs to make sense of what's going on
	//log.Printf(" from nodeId: %d Term: %d PrevLogIndex: %d", args.LeaderId, args.Term, args.PrevLogIndex)

	if args.Term < rf.curTerm {
		reply.Accepted = false
		reply.Term = rf.curTerm
		return
	}

	//if args.Term > rf.curTerm {
	//	rf.curTerm = args.Term
	//	rf.votedFor = -1
	//	rf.state = Follower
	//	return
	//}

	rf.heartbeatCh <- HeartBeatMssg{args.LeaderId, args.Term}
	//log.Printf("xxx")
	if len(args.Entries) == 0 {
		fmt.Printf("%db\n", args.LeaderId)
		//return
	}

	// prev index and term is the same, all previous logs the same
	// append all the entries when PrevLogIndex is -1 (replicate all the entries)
	// PrevLogIndex exists and term matches
	if args.PrevLogIndex == -1 || (args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.Term) {
		reply.Accepted = true
		//fmt.Printf("xxx\n")
		//fmt.Printf(" %d | %d\n",args.CommitIndex, rf.commitIndex )
		oldLen := len(rf.logs)
		rf.logs = rf.logs[:args.PrevLogIndex+1]

		rf.logs = append(rf.logs, args.Entries...)
		if len(rf.logs) > oldLen {

		log.Printf("%s(%d) log updated from length %d to %d: %v\n",rf.state,rf.me,oldLen, len(rf.logs), rf.logs)
		}

		if args.CommitIndex > rf.commitIndex {
			//fmt.Printf("Leader commit index is bigger %d | %d\n",args.CommitIndex, rf.commitIndex )
			old := rf.commitIndex
			lastNewEntry := len(rf.logs) - 1
			if args.CommitIndex < lastNewEntry {
				rf.commitIndex = args.CommitIndex
			} else {
				rf.commitIndex = lastNewEntry
			}
			fmt.Printf("%s(%d) Updating commitIndex from %d to %d\n",rf.state,rf.me,old, rf.commitIndex )
			go rf.applyLogs() //here ?
		}

		return

	}

	// Reply false if there is no value at PrevLogIndex or if doesn’t contain an entry whose term matches PrevLogTerm
	if args.PrevLogIndex > len(rf.logs)-1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Accepted = false
		return
	}

}

// reintialization of some states when a leader steps in
func (rf *Raft) startAsLeader() {
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			rf.nextIndexes[i] = -1
			rf.matchIndexes[i] = -1
			continue
		}
		// leader initialize a peer node's next index to its' last log index + 1
		rf.nextIndexes[i] = len(rf.logs)
		rf.matchIndexes[i] = 0

	}

	rf.printLeaderInfo()
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	// Your code here (2B).

	curTerm, isLeader := rf.GetState()

	if !isLeader {
		//log.Printf("node %d is not a leader to send command against\n", rf.me)
		return -1, curTerm, false
	}

	// wa want expose this function later as an rpc call, so we want to make it thread-safe
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newEntry := Log{curTerm,command}
	rf.logs = append(rf.logs, newEntry)
	log.Printf("Term %d Leader(%d)New log appended %v\n",rf.curTerm,rf.me, rf.logs)
	index = len(rf.logs) - 1
	//go rf.notifyNewLog()

	// as Start takes in a new log, it grows the size of the log by 1 i.e. last index grows by 1
	//fmt.Printf("index: %d\n", index)
	return index, curTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	// this function is blocking until the nodeId is killed (e.g. the long running goroutine stopped)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	log.Printf("Killing a %s Raft instance: %d", rf.state, rf.me)
	<-rf.killedCh
	log.Printf("Done Killing Raft instance: %d", rf.me)

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) GetRandTimeout() int32 {
	return rf.randNumGen.Int31n(rf.elecTimeoutBounds.max-rf.elecTimeoutBounds.min) + rf.elecTimeoutBounds.min
}

// main program loop for a raft instance
func (rf *Raft) Loop() {
	for {
		if rf.killed() {
			log.Printf("Raft: Node %d Killed	Term %d		State %s", rf.me, rf.curTerm, rf.state)
			rf.killedCh <- true
			return
		} else if rf.state == Follower {
			randTimeout := rf.GetRandTimeout()
			select {
			case <-rf.heartbeatCh:
				// case hearbeatMssg := <-rf.heartbeatCh:
				//log.Printf("Raft: [Id: %d | Term: %d | %v] - Heart beat from nodeId: %d", rf.me, rf.curTerm, rf.state, hearbeatMssg.nodeId)
				continue //reset the timeout
			case <-time.After(time.Duration(randTimeout) * time.Millisecond):
				//log.Printf("Raft: [Id: %d | Term: %d | %v] - Election timer timed out. Timeout: %dms", rf.me, rf.curTerm, rf.state, randTimeout)
				//not receiving a heartbeat for longer than the timeout
				// start a new election
				rf.state = Candidate
				log.Printf("Raft: Node %d timedout as Folloer	Term %d		Election starts ", rf.me, rf.curTerm)
				//log.Printf("Raft: [Id: %d | Term: %d | %v] - Election start", rf.me, rf.curTerm, rf.state)
			}

		} else if rf.state == Candidate {
			// vote for itself and ask other nodes for vote with the goal to have votes from the majority
			rf.curTerm += 1 // whenever we start a new election we increment the current Term
			voteChan := make(chan int)
			voteReplies := rf.RequestVotesCluster(voteChan)
			log.Printf("Raft: Node %d send out votes		Term %d		state: %s", rf.me, rf.curTerm, rf.state)
			voteCount := 1
			yesCount := 1
			rf.votedFor = rf.me
			majorityCount := len(rf.peers) / 2

			electionTimeout := rf.GetRandTimeout()

		votingLoop:
			for {

				select {
				case heartbeatMssg := <-rf.heartbeatCh:
					rf.state = Follower
					log.Printf("Raft: [Id: %d | Term: %d | %v] - Heart beat from nodeId: %d", rf.me, rf.curTerm, rf.state, heartbeatMssg.nodeId)
					log.Printf("Raft: [Id: %d | Term: %d | %v] - converting form candiate to follower: %d", rf.me, rf.curTerm, rf.state, heartbeatMssg.nodeId)
					break votingLoop
				case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
					// If election timeout elapses: start new election
					log.Printf("Raft: [Id: %d | Term: %d | %v] - Election timer timed out. Timeout: %dms", rf.me, rf.curTerm, rf.state, electionTimeout)
					//not receiving a heartbeat for longer than the timeout
					log.Printf("Raft: [Id: %d | Term: %d | %v] - Election start", rf.me, rf.curTerm, rf.state)
					break votingLoop // start a new election

				case voterIndex := <-voteChan:
					if (*voteReplies)[voterIndex].VoteGranted {
						yesCount += 1
					}
					voteCount += 1

					if yesCount > majorityCount {
						//log.Printf("Raft: [Id: %d | Term: %d | %v] - becomes leader", rf.me, rf.curTerm, rf.state)
						rf.state = Leader
						log.Printf("Raft: Node %d becomes leader	Term %d		state: %s", rf.me, rf.curTerm, rf.state)
						rf.startAsLeader() //toodo check bakc here
						break votingLoop
					}

				}
			}

			log.Printf("Raft: Term: %d	Leader: %d	Votes: %d/%d", rf.curTerm, rf.me, yesCount, len(rf.peers))
			//log.Printf("Raft: [Id: %d | Term: %d | %v] - Election results. Vote: %d/%d", rf.me, rf.curTerm, rf.state, yesCount, len(rf.peers))

		} else if rf.state == Leader {
			//go rf.sendClusterAppendEntries(170 * time.Millisecond)
			rf.sendClusterAppendEntries()
			time.Sleep(150 * time.Millisecond)

		} else {
			fmt.Println("Unknown instance state")
		}

	}

}

// ask the entire cluster for votes
// return a preallocated reply slice that will later be populated once
// as we are getting votes from the peers
func (rf *Raft) RequestVotesCluster(voteChan chan int) *[]RequestVoteReply {
	rpcArgs := &RequestVoteArgs{
		CandidID:     rf.me,
		Term:         rf.curTerm, // assumes Term no. has already been incremented before calling this function
		LastLogTerm:  getLastLogTerm(rf),
		LastLogIndex: getLastLogIndex(rf),
	}

	voteReplies := make([]RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendRequestVote(i, rpcArgs, &voteReplies[i], voteChan)
	}

	return &voteReplies

}

type HeartBeatMssg struct {
	nodeId int // nodeId which sent the heartbeat
	term   int
}

func (rf *Raft) sendClusterAppendEntries() {
	// either sends out heartbeat(i.e. AppendEntry with no entries)
	// or send actual entries
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			args := AppendEntryArgs{}
			args.Term = rf.curTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndexes[i] - 1
			//fmt.Println("args.PrevLogIndex:", args.PrevLogIndex)
			args.CommitIndex = rf.commitIndex
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}

			// more logs could be added between the time we initialize the leader and the time we send out
			// the new entries
			args.Entries = rf.logs[rf.nextIndexes[i]:]
			args.Term = rf.curTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndexes[i] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			} else {
				args.PrevLogTerm = -1
			}
			// when there is no new logs, args.Entries becomes empty
			// when there is new logs args.Entrie contains the new logs
			args.Entries = rf.logs[rf.nextIndexes[i]:]
			go rf.sendAppendEntry(i, &args, &AppendEntryReply{})

		}
	}

}

// a leader can call this method to stop heart beats
// it should be called before converting a leader to a follwoer
func (rf *Raft) stopHeartBeats() {
	rf.stopHeartbeatCh <- true
}

func (rf *Raft) applyLogs() {
	log.Printf("%s(%d) Updating/Applying lastApplied lastApplied: %d\t\tcommitIndex %d\n",rf.state, rf.me,rf.lastApplied, rf.commitIndex)
	//log.Printf("node %d log: %v\n",rf.me, rf.logs)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.logs[i].Command, CommandValid: true}
	}
	rf.lastApplied = rf.commitIndex
	log.Printf("%s(%d)	lastApplied: %d		commitIndex %d\n",rf.state, rf.me, rf.lastApplied, rf.commitIndex)
}


//helper functions
func getLastLogTerm(rf *Raft) int {
	if len(rf.logs) == 0 {
		return -1
	}
	return rf.logs[len(rf.logs)-1].Term
}

func getLastLogIndex(rf *Raft) int {
	if len(rf.logs) == 0 {
		return -1
	}
	return len(rf.logs) - 1

}

func (rf *Raft) printLeaderInfo() {
	log.Printf("Leader info:\n")
	fmt.Printf("Leader Node: %d		Term: %d\n", rf.me, rf.curTerm)
	fmt.Printf("			commitIndex: %d\n", rf.commitIndex)
	fmt.Printf("			lastApplied: %d\n", rf.lastApplied)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		me:        me,
		curTerm:  0,
		state:    Follower,
		votedFor: -1,
		logs: []Log{},
		commitIndex: -1,
		peers:     peers,
		lastApplied: -1,
		elecTimeoutBounds: ElecTimeoutBounds{
			min: 250,
			max: 600,
		},
		randNumGen:  rand.New(rand.NewSource(time.Now().UnixNano())),
		heartbeatCh: make(chan HeartBeatMssg),
		newLogCh:    make(chan bool, 1),
		killedCh:    make(chan bool, 1),
		applyCh:     applyCh,
		persister: persister,
	}

	log.Printf("Raft: Node %d created	Term %d		State %s", rf.me, rf.curTerm, rf.state)
	// Your initialization code here (2A, 2B, 2C).
	go rf.Loop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
