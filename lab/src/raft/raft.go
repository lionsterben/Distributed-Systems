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
	"fmt"
	"labgob"
	"sync"
)
import (
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "labgob"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	log          []Log //slice,index从1开始
	commitIndex  int
	lastApplied  int
	currentState string
	NextIndex    []int
	matchIndex   []int
	voteCount    int
	commitCount  int
	electWin     chan bool
	grant        chan bool
	heartBeat    chan bool
	applyCh      chan ApplyMsg
	//newLog 		 chan Log
}

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.mu.Lock()
	term = rf.currentTerm
	fmt.Printf("%d rf_term %d rf_state %s rf_isvotecount %d \n", rf.me, term, rf.currentState, rf.voteCount)
	//fmt.Printf("rf_state %s\n",rf.currentState)
	//fmt.Printf("rf_istimeout %d\n",rf.isTimeout)
	isleader = rf.currentState == "leader"
	//rf.mu.Unlock()
	return term, isleader
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil{
			panic("wrong")
	}else{
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d vote for %d\n", rf.me, args.CandidateId)
	//fmt.Printf("%d vote_rf_term %d\n",rf.me,rf.currentTerm)
	//isgrant := false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = "follower"
		rf.votedFor = -1
		rf.voteCount = 0
		rf.persist()
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUptodate(args.LastLogIndex, args.LastLogTerm) {
		rf.grant <- true //用来给follower跳出等待
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	//rf.persist()

}

func (rf *Raft) isUptodate(candIndex int, candTerm int) bool {
	index := len(rf.log) - 1
	term := rf.log[len(rf.log)-1].Term
	return candTerm > term || (candTerm == term && candIndex >= index)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { //接收回信息之后，对发送者自身进行处理
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.currentState == "candidate" && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = "follower"
			rf.votedFor = -1
			rf.voteCount = 0
			rf.persist()
			return ok
		}
		if reply.VoteGranted {
			//fmt.Printf("from %d vote reply %d %t\n", k, voteReply.Term, voteReply.VoteGranted)
			//fmt.Printf("vote reply currentterm %d \n", rf.currentTerm)
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				rf.currentState = "leader"
				rf.electWin <- true
			}
		}
	}
	// else {
	//	return ok
	//}
	//rf.persist()
	return ok
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != "leader" {
		isLeader = false
		return index, term, isLeader
	}
	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, Log{command, term})
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//define an AppendEntries RPC struct (though you may not need all the arguments yet)
//lab2A only finish heart beat mechanism
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Enteries     []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//todo
	//心跳机制
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if args.enteries == nil{//可以看成心跳机制
	//	reply.success = true
	//	reply.term = rf.currentTerm
	//}
	//success := false
	//fmt.Printf("dhuiawhfiawhfoawfh %d %d\n",args.Term,rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.commitIndex >= args.PrevLogIndex+len(args.Enteries) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.commitIndex + 1
		rf.heartBeat <- true
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = "follower"
		rf.voteCount = 0
		rf.votedFor = -1
	}
	rf.heartBeat <- true
	reply.Term = rf.currentTerm
	rfLogLength := rf.getLastLogIndex()
	if rfLogLength < args.PrevLogIndex {
		reply.NextIndex = rfLogLength + 1
		reply.Success = false
		return
	}
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { //不匹配
		// reply.NextIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i > 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.NextIndex = i + 1
				break
			}
		}
		reply.Success = false
	} else { //匹配
		startInd := args.PrevLogIndex + 1
		rf.log = rf.log[:startInd]
		rf.log = append(rf.log, args.Enteries...)
		reply.Success = true
		reply.NextIndex = args.PrevLogIndex + len(args.Enteries)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rfLogLength {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rfLogLength
			}
			go rf.applyLog()
		}
	}
	rf.persist()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != "leader" || rf.currentTerm != args.Term || !ok {
		return ok
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = "follower"
			rf.voteCount = 0
			rf.votedFor = -1
			return ok
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Enteries)
			rf.NextIndex[server] = rf.matchIndex[server] + 1
			for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
				count := 1
				if rf.log[i].Term == rf.currentTerm {
					for j := range rf.peers {
						if j != rf.me && rf.matchIndex[j] >= i {
							count++
						}
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					go rf.applyLog()
					break
				}
			}
		} else {
			rf.NextIndex[server] = reply.NextIndex
		}
		rf.persist()
	}
	//rf.persist()
	return ok
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
	}
	rf.lastApplied = rf.commitIndex
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

func (rf *Raft) sendAllLog() { //检测是否为leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for k := range rf.peers {
		if k != rf.me && rf.currentState == "leader" {
			fmt.Printf("from leader %d heartBeat to %d\n", rf.me, k)
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.NextIndex[k] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			if rf.getLastLogIndex() >= rf.NextIndex[k] {
				args.Enteries = rf.log[rf.NextIndex[k]:]
			}
			args.LeaderCommit = rf.commitIndex
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(k, args, reply)
			//rf.mu.Lock()
			//if heartBeatReply.Term > rf.currentTerm{
			//	rf.currentTerm = heartBeatReply.Term
			//	rf.currentState = "follower"
			//}
			//rf.mu.Unlock()

		}
	}
	rf.persist()

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft { //log数组起个头文件？
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.log[0] = Log{"nil", -1}
	rf.currentState = "follower"
	rf.heartBeat = make(chan bool)
	rf.grant = make(chan bool)
	//rf.heartBeat = make(chan bool)
	rf.electWin = make(chan bool)
	rf.applyCh = applyCh
	rf.voteCount = 0
	//rf.newLog = make(chan Log)
	// initialize from state persisted before a crash
	//electElapse := rand.Int() % 1000
	rf.readPersist(persister.ReadRaftState())
	//todo
	//超时后转为candidate，发送request vote，转为leader
	fmt.Printf("%d peers:%d \n", rf.me, len(rf.peers))
	go func() { //监控选举
		for {
			switch rf.currentState {
			case "leader":
				rf.sendAllLog()
				time.Sleep(time.Duration(50+rand.Int()%100) * time.Millisecond)
			case "candidate":
				args := &RequestVoteArgs{}
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.voteCount = 1
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.log) - 1
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
				rf.mu.Unlock()
				for k := range rf.peers {
					if k != rf.me {
						go rf.sendRequestVote(k, args, &RequestVoteReply{})
					}
				}
				fmt.Printf("rf current state %s rf %d start send\n", rf.currentState, rf.me)
				select {
				case <-time.After(time.Duration(time.Duration(rand.Intn(500)+500) * time.Millisecond)):
				case <-rf.heartBeat:
					rf.currentState = "follower"
					rf.voteCount = 0
				case <-rf.electWin:
					rf.mu.Lock()
					rf.currentState = "leader"
					rf.NextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for k := range rf.NextIndex {
						rf.NextIndex[k] = len(rf.log)
					}
					rf.mu.Unlock()
					rf.sendAllLog()
				}
			case "follower":
				select {
				case <-rf.grant:
				case <-rf.heartBeat:
				case <-time.After(time.Duration(time.Duration(rand.Intn(500)+500) * time.Millisecond)):
					rf.currentState = "candidate"
				}
			}
		}
	}()

	return rf
}
