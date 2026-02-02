package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l
	return lk
}

/**
* Distributed Key Value Store Implementation
* We store a lock "l" in the existing KV store using the Clerks via GET and PUT operations
* It is a distributed lock because the lock is stored in a central location, but accessed by distributed clients
* We perform a Compare and Swap operation to ensure that the lock is correctly populated.
 */

func (lk *Lock) Acquire() {
	// Your code here
	// Implementing something like a Compare-and-Swap (CAS)
	// i.e comparing a memory location's current value to expected value and if its the same then we change the value
	for {
		val, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			if lk.ck.Put(lk.lockKey, "x", 0) == rpc.OK {
				return
			}
		} else if val == "" {
			if lk.ck.Put(lk.lockKey, "x", version) == rpc.OK {
				return
			}
		}
		// spin and retry in case there is any (rpc) network issues
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			return
		} else if val == "x" {
			if lk.ck.Put(lk.lockKey, "", version) == rpc.OK {
				return
			}
		}
		// spin and retry in case there is rpc network issues
	}
}
