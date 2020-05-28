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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
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
	term int
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
	logs    []Log
	state   NodeState
	curTerm int

	//election timeout states
	elecTimeoutBounds ElecTimeoutBounds
	randNumGen        *rand.Rand // random number generator that seed is initialized with time.Now().UnixNano()

	//candidate states
	votedFor        int // index for the candidate this nodeId voted for in its current Term, -1 indicates not voted yet
	heartbeatCh     chan HeartBeatMssg
	killedCh        chan bool
	stopHeartbeatCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	term, isLeader = rf.curTerm, rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type NodeState uint32

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
	log.Printf("vvvv")

	isCandidLogsMoreUpToDate := func() bool {
		// Raft determines which of two logs is more up-to-date
		// by comparing the index and Term of the last entries in the
		// logs. If the logs have last entries with different terms, then
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

	switch {
	case args.Term < rf.curTerm:
		reply.VoteGranted = false
		reply.Term = rf.curTerm
	// all cases bellow have argument's Term >= to rf.curTerm
	case args.Term > rf.curTerm:
		reply.VoteGranted = true
		rf.votedFor = args.CandidID
		rf.curTerm = args.Term
		if rf.state == Leader {
			rf.stopHeartBeats()
		}
		rf.state = Follower
	case (rf.votedFor == -1 || rf.votedFor == args.CandidID) && isCandidLogsMoreUpToDate:
		// this nodeId has either not voted or has already voted this candidate
		reply.VoteGranted = true
		rf.votedFor = args.CandidID
	default:
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		log.Printf("Request Vote no cases satisfied.\n")

	}

	log.Printf("Node %d asks nodeId %d to vote: Node %d's(candidate) term : %d Node %d's term : %d  log up-to-date? %v. Vote granted? %v", args.CandidID, rf.me, args.CandidID, args.Term, rf.me, rf.curTerm, isCandidLogsMoreUpToDate, reply.VoteGranted)
	//log.Printf("votedfor: %d | rf.term | args.term: %d | %d", rf.votedFor, rf.curTerm, args.Term)

}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
}

type AppendEntryReply struct {
	Term int
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	//if !ok {
	//	log.Printf("Raft: [Id: %d | Term: %d | %v] - Communication error: AppendEntries() RPC failed", rf.me, rf.curTerm, rf.state)
	//}
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	log.Printf("Heartbeat received on nodeId: %d from nodeId: %d Term: %d", rf.me, args.LeaderId, args.Term)
	//log.Printf("xxx")
	rf.heartbeatCh <- HeartBeatMssg{args.LeaderId, args.Term}
	//log.Printf("xxx")
	reply.Term = rf.curTerm

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
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
		//log.Printf("xxx")
		//log.Printf("Start of the loop")
		if rf.killed() {
			rf.killedCh <- true
			return

		} else if rf.state == Follower {
			randTimeout := rf.GetRandTimeout()
			select {
			case hearbeatMssg := <-rf.heartbeatCh:
				log.Printf("Raft: [Id: %d | Term: %d | %v] - Heart beat from nodeId: %d", rf.me, rf.curTerm, rf.state, hearbeatMssg.nodeId)
				continue //reset the timeout
			case <-time.After(time.Duration(randTimeout) * time.Millisecond):
				log.Printf("Raft: [Id: %d | Term: %d | %v] - Election timer timed out. Timeout: %dms", rf.me, rf.curTerm, rf.state, randTimeout)
				//not receiving a heartbeat for longer than the timeout
				// start a new election
				rf.state = Candidate
				log.Printf("Raft: [Id: %d | Term: %d | %v] - Election start", rf.me, rf.curTerm, rf.state)
			}

		} else if rf.state == Candidate {
			// vote for itself and ask other nodes for vote with the goal to have votes from the majority
			rf.curTerm += 1 // whenever we start a new election we increment the current term
			voteChan := make(chan int)
			voteReplies := rf.RequestVotesCluster(voteChan)
			voteCount := 1
			yesCount := 1
			rf.votedFor = rf.me
			majorityCount := len(rf.peers) / 2

			randTimeout := rf.GetRandTimeout()

		votingLoop:
			for {

				select {
				case heartbeatMssg := <-rf.heartbeatCh:
					rf.state = Follower
					log.Printf("Raft: [Id: %d | Term: %d | %v] - Heart beat from nodeId: %d", rf.me, rf.curTerm, rf.state, heartbeatMssg.nodeId)
					log.Printf("Raft: [Id: %d | Term: %d | %v] - converting form candiate to follower: %d", rf.me, rf.curTerm, rf.state, heartbeatMssg.nodeId)
					break votingLoop
				case <-time.After(time.Duration(randTimeout) * time.Millisecond):
					log.Printf("Raft: [Id: %d | Term: %d | %v] - Election timer timed out. Timeout: %dms", rf.me, rf.curTerm, rf.state, randTimeout)
					//not receiving a heartbeat for longer than the timeout
					log.Printf("Raft: [Id: %d | Term: %d | %v] - Election start", rf.me, rf.curTerm, rf.state)
					break votingLoop // start a new election

				case voterIndex := <-voteChan:
					if (*voteReplies)[voterIndex].VoteGranted {
						yesCount += 1
					}
					voteCount += 1

					if yesCount > majorityCount {
						rf.state = Leader
						log.Printf("Raft: [Id: %d | Term: %d | %v] - becomes leader", rf.me, rf.curTerm, rf.state)
						go rf.HeartbeatLoop(170 * time.Millisecond)
						return
					}

				}
			}

			log.Printf("Raft: [Id: %d | Term: %d | %v] - Election results. Vote: %d/%d", rf.me, rf.curTerm, rf.state, yesCount, len(rf.peers))

		} else if rf.state == Leader {

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

func (rf *Raft) HeartbeatLoop(timeout time.Duration) {
	for {
		select {
		case <-rf.stopHeartbeatCh:
			return
		case heartBeatMssg := <-rf.heartbeatCh:
			if heartBeatMssg.term > rf.curTerm {
				log.Printf("leader converting to follower")
				rf.state = Follower
				rf.curTerm = heartBeatMssg.term
				return
			}

		case <-time.After(timeout):
			if rf.killed() {
				rf.killedCh <- true
				return
			}
			log.Printf("Raft: [Id: %d | Term: %d | %v] - Sending heartbeats to cluster", rf.me, rf.curTerm, rf.state)
			rf.heartbeatALl()
		}
	}

}

func (rf *Raft) heartbeatALl() {
	args := AppendEntryArgs{Term: rf.curTerm, LeaderId: rf.me}
	resps := make([]AppendEntryReply, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntry(i, &args, &resps[i])
	}
}

// a leader can call this method to stop heart beats
// it should be called before converting a leader to a follwoer
func (rf *Raft) stopHeartBeats() {
	rf.stopHeartbeatCh <- true
}

//simple helper functions
func getLastLogTerm(rf *Raft) int {
	if len(rf.logs) == 0 {
		return -1
	}
	return rf.logs[len(rf.logs)-1].term
}

func getLastLogIndex(rf *Raft) int {
	if len(rf.logs) == 0 {
		return -1
	}
	return len(rf.logs) - 1

}

type HeartBeatMssg struct {
	nodeId int // nodeId which sent the heartbeat
	term   int
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
		peers:     peers,
		me:        me,
		persister: persister,
		/////////////////////
		state:    Follower,
		curTerm:  0,
		votedFor: -1,
		elecTimeoutBounds: ElecTimeoutBounds{
			min: 250,
			max: 600,
		},
		randNumGen:  rand.New(rand.NewSource(time.Now().UnixNano())),
		heartbeatCh: make(chan HeartBeatMssg),
		killedCh:    make(chan bool, 1),
	}

	log.Printf("Raft: [Id: %d | Term: %d | %v] - Node created", rf.me, rf.curTerm, rf.state)
	// Your initialization code here (2A, 2B, 2C).
	go rf.Loop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
