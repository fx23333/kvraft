package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

func Min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

type Clerk struct {
	servers        []*labrpc.ClientEnd
	possibleLeader int
	mu             sync.Mutex
	Id             int64
	Sequence       int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.possibleLeader = -1
	ck.Sequence = 1
	ck.Id = nrand()
	DPrintf("hihi-------------------------------------------------------------------")
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("Get GET request of %v", key)
	seq := ck.Sequence

	if ck.possibleLeader == -1 {
		return NoLeader(key, "", "Get", ck, seq)
	} else {
		success, returnedValue := SendingGetRpc(key, ck.possibleLeader, ck, seq)
		if !success {
			return NoLeader(key, "", "Get", ck, seq)
		} else {
			return returnedValue
		}
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func SendingPutAppendRpc(key string, value string, op string, leaderId int, ck *Clerk, seq int) bool {
	//ck.mu.Lock()
	if leaderId == -1 {
		panic("Leader CANNOT Be -1")
	}
	if op == "Get" {
		panic("wrong type")
	}
	DPrintf("current Leader = %d, %v(%v, %v)\n", leaderId, op, key, value)
	args := &PutAppendArgs{Op: op, Key: key, Value: value, ClientId: ck.Id, Sequence: seq}
	reply := new(PutAppendReply)
	ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
	retryCount := 0

	for !ok && retryCount < 10 {
		retryCount++
		ok = ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
		time.Sleep(time.Duration(20) * time.Millisecond)
		//panic("hi")
	}

	if retryCount >= 10 {
		//ck.mu.Unlock()
		return false
		panic("CLIENT FUNC(SendingPutAppendRpc): COULD NOT SENDING RPC")
	}

	// 此时至少收到了RPC
	if reply.Err == ErrWrongLeader {
		ck.possibleLeader = -1
		//ck.mu.Unlock()
		DPrintf("Received ErrLeader\n")
		return false
	} else if reply.Err == OK {
		ck.possibleLeader = leaderId
		DPrintf("Received OK, seq[%v]:%v   %v(%v, %v)\n", leaderId, seq, op, key, value)
		//ck.mu.Unlock()
		return true
	} else if reply.Err == ErrCompleted {
		ck.possibleLeader = leaderId
		DPrintf("Received ErrCompleted, seq[%v]:%v\n", leaderId, seq)
		//ck.mu.Unlock()
		return true
	}

	panic("Should Not Reach Here")
	//ck.mu.Unlock()
	return false
}
func SendingGetRpc(key string, leaderId int, ck *Clerk, seq int) (bool, string) {
	//ck.mu.Lock()
	if leaderId == -1 {
		panic("Leader CANNOT Be -1")
	}
	args := &GetArgs{Key: key, ClientId: ck.Id, Sequence: seq}
	reply := new(GetReply)
	ok := ck.servers[leaderId].Call("KVServer.Get", args, reply)
	retryCount := 0
	for !ok && retryCount < 10 {
		retryCount++
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
	// panic("hihihihihhihi")
	if retryCount == 10 {
		//ck.mu.Unlock()
		return false, ""
		panic("CLIENT FUNC(SendingGetRpc): COULD NOT SENDING RPC")
	}
	if reply.Err == ErrWrongLeader {
		DPrintf("Get: ErrWrongLeader")
		//ck.mu.Unlock()
		ck.possibleLeader = -1
		return false, "fx2333"
	} else if reply.Err == ErrNoKey {

		// 此时代表无该Key
		DPrintf("Get: ErrNoKey")
		//ck.mu.Unlock()
		return true, ""
	} else if reply.Err == OK {
		ck.possibleLeader = leaderId
		//ck.mu.Unlock()
		DPrintf("Get: OK")
		return true, reply.Value
	} else if reply.Err == ErrCompleted {
		//ck.mu.Unlock()
		DPrintf("Get: ErrCompleted")
		return false, "abcdabcd"
	}

	panic("CLIENT FUNC(SendingGetRpc): CANNOT REACH HERE")
}

func NoLeader(key string, value string, op string, ck *Clerk, seq int) string {
	count := 0
	var success bool
	var valueReturned string
	upBound := 100000000
	if op == "Get" {
		// panic("hi")
		success, valueReturned = SendingGetRpc(key, 0, ck, seq)
		i := 1
		for ; !success && count < upBound; i = (i + 1) % len(ck.servers) {

			count++
			DPrintf("Get i = %d*****************************************\n", i)
			success, valueReturned = SendingGetRpc(key, i, ck, seq)
			time.Sleep(time.Duration(200) * time.Millisecond)

		}
	} else {
		for i := 0; !SendingPutAppendRpc(key, value, op, i, ck, seq) && count < upBound; i = (i + 1) % len(ck.servers) {
			count++
			DPrintf("Send i = %d*****************************************\n", i)
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}

	if count >= upBound {
		panic("CANNOT FIND THE LEADER! CHECK THE LOGIC OF FUNC(NoLEADER)!  " + op)
	} else if op == "Get" {
		return valueReturned
	} else {
		return ""
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//ck.mu.Lock()
	seq := ck.Sequence
	ck.Sequence++
	//ck.mu.Unlock()
	if ck.possibleLeader == -1 || !SendingPutAppendRpc(key, value, op, ck.possibleLeader, ck, seq) {
		NoLeader(key, value, op, ck, seq)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
