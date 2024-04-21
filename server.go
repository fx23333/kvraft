package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const TIMEOUT = 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Value    string
	Key      string
	ClientId int64
	Sequence int
	Term     int
}

type KVServer struct {
	mu                   sync.Mutex
	me                   int
	rf                   *raft.Raft
	applyCh              chan raft.ApplyMsg
	dead                 int32 // set by Kill()
	data                 map[string]string
	peers                []*labrpc.ClientEnd
	maxraftstate         int // snapshot if log grows this big
	sequenceOnEachClient map[int64]int
	//cond                 *sync.Cond
	//condMu               sync.Mutex
	//currentApplyMsg      raft.ApplyMsg
	rpcChannel map[int]chan Op // log index -> the channel of a RPC
	persister  *raft.Persister
	// Your definitions here.
}

//func (kv *KVServer) Execute(args *PutAppendArgs) {
//
//	if args.Op == "Put" {
//		kv.data[args.Key] = args.Value
//	} else if args.Op == "Append" {
//		kv.data[args.Key] = kv.data[args.Key] + args.Value
//	} else {
//		panic("KVSERVER FUNC(Excute): WRONG OPERATION")
//	}
//}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("server(%d) = %v(%v, %v)\n", kv.me, args.Op, args.Key, args.Value)
	// kv.rf.GetState()
	logEntry := Op{Type: args.Op, Value: args.Value, Key: args.Key, ClientId: args.ClientId, Sequence: args.Sequence}
	index, _, isLeader := kv.rf.Start(logEntry)
	DPrintf("server(%d) = hello\n", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// reply.NewLeader = -1
		return
	}

	kv.mu.Lock()
	kv.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := kv.rpcChannel[index-1]
	kv.mu.Unlock()

	select {
	case logEntryReturnedFromRaft := <-waitingChannel:

		if logEntryReturnedFromRaft != logEntry {
			reply.Err = ErrWrongLeader
			DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else {

			reply.Err = OK
		}
	case <-time.After((TIMEOUT) * time.Millisecond):
		reply.Err = ErrWrongLeader
		DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	// close(waitingChannel)
	//panic("")
	// DPrintf("The value of [0~4] is %v, %v, %v, %v, %v", kv.data["0"], kv.data["1"], kv.data["2"], kv.data["3"], kv.data["4"])
	kv.mu.Lock()
	delete(kv.rpcChannel, index-1)
	kv.mu.Unlock()

}

//func (kv *KVServer) Ticker() {
//	for {
//		time.Sleep(time.Duration(TIMEOUT) * time.Millisecond)
//		kv.cond.Broadcast()
//	}
//
//}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// kv.rf.GetState()
	DPrintf("server(%d)get = %v\n", kv.me, args.Key)

	logEntry := Op{Type: "Get", Key: args.Key}

	index, _, isLeader := kv.rf.Start(logEntry)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := kv.rpcChannel[index-1]
	kv.mu.Unlock()
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.Type != logEntry.Type || logEntryReturnedFromRaft.Key != logEntry.Key {
			reply.Err = ErrWrongLeader
			DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else {
			reply.Err = OK
			DPrintf("Get(%v) From Server(%v) OK:%v", args.Key, kv.me, logEntryReturnedFromRaft.Value)
			//reply.Value = logEntryReturnedFromRaft.Value
			reply.Value = kv.data[logEntry.Key]
		}
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
		DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	kv.mu.Lock()
	delete(kv.rpcChannel, index-1)
	kv.mu.Unlock()

}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func DaemonOfApplying(kv *KVServer) {
	for entry := range kv.applyCh {
		// time.Sleep(TIMEOUT / 10 * time.Millisecond)
		kv.mu.Lock()
		//kv.currentApplyMsg = entry
		if !entry.CommandValid {
			// panic("")
			r := bytes.NewBuffer(entry.Snapshot)
			d := labgob.NewDecoder(r)
			err := d.Decode(&kv.data)
			if err != nil {
				panic("Server Decode Error!" + err.Error())
			}
			DPrintf("data:%v", kv.data)

			err = d.Decode(&kv.sequenceOnEachClient)
			if err != nil {
				panic("Server Decode Error!" + err.Error())
			}
			// panic("")
			kv.mu.Unlock()
			continue
		}
		if entry.Command.(Op).Type != "Get" {
			if kv.sequenceOnEachClient[entry.Command.(Op).ClientId] < entry.Command.(Op).Sequence {
				switch entry.Command.(Op).Type {
				case "Put":
					kv.data[entry.Command.(Op).Key] = entry.Command.(Op).Value
				case "Append":
					//panic(kv.data[entry.Command.(Op).Key] + entry.Command.(Op).Value)
					kv.data[entry.Command.(Op).Key] = kv.data[entry.Command.(Op).Key] + entry.Command.(Op).Value
				default:
					panic("KVSERVER FUNC(Excute): WRONG OPERATION")
				}
				kv.sequenceOnEachClient[entry.Command.(Op).ClientId] = raft.Max(kv.sequenceOnEachClient[entry.Command.(Op).ClientId], entry.Command.(Op).Sequence)
			}

		}

		waitingChannelOnRpc, ok := kv.rpcChannel[entry.CommandIndex-1]

		if ok {
			select {
			case waitingChannelOnRpc <- entry.Command.(Op):
				//case <-time.After((TIMEOUT / 10) * time.Millisecond):
			}
			// kick their ass
			delete(kv.rpcChannel, entry.CommandIndex-1)
		}

		kv.mu.Unlock()
		SnapShot(kv, entry.CommandIndex-1)
	}
}

//func ApplyLogEntryToChannelWaitedRpc(waitingChannelOnRpc chan Op, logEntry Op) {
//	select {
//	case waitingChannelOnRpc <- logEntry:
//	case <-time.After(TIMEOUT * time.Millisecond):
//	}
//}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.sequenceOnEachClient = make(map[int64]int)
	kv.peers = servers
	kv.data = make(map[string]string)
	kv.persister = persister
	DPrintf("Total:%d, me = %d\n", len(servers), me)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 2)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// kv.cond = sync.NewCond(&kv.condMu)
	kv.rpcChannel = make(map[int]chan Op)
	// go kv.Ticker()
	for idx, value := range servers {
		DPrintf("%v: %v  --Server\n", idx, *value)
	}
	go DaemonOfApplying(kv)
	//if maxraftstate != -1 {
	//	go DaemonOfSnapShot()
	//}
	// You may need initialization code here.
	// persister.RaftStateSize()
	return kv
}

// snapshot应该包括当前index
func SnapShot(kv *KVServer, index int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= Max(kv.maxraftstate-5, 1) {
		DPrintf("server starts snapshot! me = %v, Logsize = %v", kv.me, kv.persister.RaftStateSize())
		//panic("")
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		var err error
		err = e.Encode(kv.data)
		if err != nil {
			panic("kv Snapshot error 1" + err.Error())
		}

		err = e.Encode(kv.sequenceOnEachClient)
		if err != nil {
			panic("kv Snapshot error 2" + err.Error())
		}
		Snapshot := w.Bytes()
		kv.rf.ApplySnapshot(Snapshot, index)
		DPrintf("server starts snapshot over! me = %v, Logsize = %v", kv.me, kv.persister.RaftStateSize())
	}
}
