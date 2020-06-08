### Overview
A raft implementation based on the paper "Search of an Understandable Consensus Algorithm". Features implemented:

- leader election with ramdom timeout
- heartbeat(empty log append) and log replication across nodes

A running cluster's log may look like this:
```
2020/06/08 06:21:42 Raft: Node 0 created        Term 0          State Follower
2020/06/08 06:21:42 Raft: Node 1 created        Term 0          State Follower
2020/06/08 06:21:42 Raft: Node 2 created        Term 0          State Follower
Test (2B): agreement despite follower disconnection ...
2020/06/08 06:21:43 Follower(1) timed out. New election start New Term: 1
2020/06/08 06:21:43 Raft: Node 1 send out votes         Term 1          state: Candidate
2020/06/08 06:21:43 node 1(Term 1) requests node 0(Term 0) to vote vote granted: true
2020/06/08 06:21:43 node 1(Term 1) requests node 2(Term 0) to vote vote granted: true
2020/06/08 06:21:43 Raft: Node 1 becomes leader Term 1          state: Leader
2020/06/08 06:21:43 Leader info:
Leader Node: 1          Term: 1
                        commitIndex: 0
                        lastApplied: 0
2020/06/08 06:21:43 Voting Result: Term: 1      Leader: 1       Votes: 2/3
1b
1b
2020/06/08 06:21:43 Term 1 Leader(1)New log appended {1 101}
2020/06/08 06:21:43 Follower(0) log updated from length 1 to 2: [{0 <nil>} {1 101}]
Updating leader(1) commitIndex from 0 to 1
2020/06/08 06:21:43 Leader(1) Updating/Applying lastApplied lastApplied: 0              commitIndex 1
2020/06/08 06:21:43 Follower(2) log updated from length 1 to 2: [{0 <nil>} {1 101}]
Follower(2) Updating commitIndex from 0 to 1
2020/06/08 06:21:43 Follower(2) Updating/Applying lastApplied lastApplied: 0            commitIndex 1
Follower(0) Updating commitIndex from 0 to 1
2020/06/08 06:21:43 Follower(0) Updating/Applying lastApplied lastApplied: 0            commitIndex 1
discon the node 2
2020/06/08 06:21:44 Term 1 Leader(1)New log appended {1 102}
2020/06/08 06:21:44 Follower(0) log updated from length 2 to 3: [{0 <nil>} {1 101} {1 102}]
Updating leader(1) commitIndex from 1 to 2
2020/06/08 06:21:44 Leader(1) Updating/Applying lastApplied lastApplied: 1              commitIndex 2
Follower(0) Updating commitIndex from 1 to 2
2020/06/08 06:21:44 Follower(0) Updating/Applying lastApplied lastApplied: 1            commitIndex 2
2020/06/08 06:21:44 Term 1 Leader(1)New log appended {1 103}
2020/06/08 06:21:44 Follower(0) log updated from length 3 to 4: [{0 <nil>} {1 101} {1 102} {1 103}]
Updating leader(1) commitIndex from 2 to 3
```


more info: [https://pdos.csail.mit.edu/6.824/labs/lab-raft.html](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
