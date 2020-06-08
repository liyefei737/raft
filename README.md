### Overview
A raft implementation based on the paper "Search of an Understandable Consensus Algorithm". Features implemented:

- leader election with ramdom timeout
- heartbeat(empty log append) and log replication across nodes

A running cluster's log may look like this:
```
2020/06/08 06:48:23 Raft: Node 0 created        Term 0          State Follower
2020/06/08 06:48:23 Raft: Node 1 created        Term 0          State Follower
2020/06/08 06:48:23 Raft: Node 2 created        Term 0          State Follower
Test (2B): basic agreement ...
2020/06/08 06:48:23 Follower(2) timed out. New election start New Term: 1
2020/06/08 06:48:23 Raft: Node 2 send out votes         Term 1          state: Candidate
2020/06/08 06:48:23 node 2(Term 1) requests node 0(Term 0) to vote vote granted: true
 
2020/06/08 06:48:23 node 2(Term 1) requests node 1(Term 0) to vote vote granted: true
 
2020/06/08 06:48:23 Raft: Node 2 becomes leader Term 1          state: Leader
2020/06/08 06:48:23 Leader info:
Leader Node: 2          Term: 1
                        commitIndex: 0
                        lastApplied: 0
2020/06/08 06:48:23 Voting Result: Term: 1      Leader: 2       Votes: 2/3
2020/06/08 06:48:23 2b
2020/06/08 06:48:23 2b
2020/06/08 06:48:23 Term 1 Leader(2)New log appended {1 0}
2020/06/08 06:48:23 Follower(1) log updated from length 1 to 2: [{0 <nil>} {1 0}]
2020/06/08 06:48:23 Updating leader(2) commitIndex from 0 to 1
2020/06/08 06:48:23 Follower(0) log updated from length 1 to 2: [{0 <nil>} {1 0}]
2020/06/08 06:48:23 Leader(2) Updating/Applying lastApplied lastApplied: 0              commitIndex 1
2020/06/08 06:48:23 Follower(1) Updating commitIndex from 0 to 1
2020/06/08 06:48:23 Follower(0) Updating commitIndex from 0 to 1
2020/06/08 06:48:23 Follower(1) Updating/Applying lastApplied lastApplied: 0            commitIndex 1
2020/06/08 06:48:23 Follower(0) Updating/Applying lastApplied lastApplied: 0            commitIndex 1
2020/06/08 06:48:23 Raft: Node 0 Killed Term 1          State Follower
2020/06/08 06:48:23 Killed a Follower Raft instance: 0
2020/06/08 06:48:23 Raft: Node 1 Killed Term 1          State Follower
2020/06/08 06:48:23 Killed a Follower Raft instance: 1
2020/06/08 06:48:23 Raft: Node 2 Killed Term 1          State Leader
2020/06/08 06:48:23 Killed a Leader Raft instance: 2
```


more info: [https://pdos.csail.mit.edu/6.824/labs/lab-raft.html](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
