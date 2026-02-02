package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionedValue struct {
	version rpc.Tversion
	value   string // adapted from the PUTArgs of GET and PUT rpcs
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	keyValueMap map[string]VersionedValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.

	kv.keyValueMap = make(map[string]VersionedValue)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key

	// lock access to critical section which is the kv map
	kv.mu.Lock()
	defer kv.mu.Unlock()

	versionedVal, exists := kv.keyValueMap[key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = versionedVal.value
	reply.Version = versionedVal.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	// check if key exists

	version := args.Version
	value := args.Value
	versionedVal, exists := kv.keyValueMap[key]
	if !exists {
		if version == 0 {
			kv.keyValueMap[key] = VersionedValue{
				version: 1,
				value:   value,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	if versionedVal.version != version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.keyValueMap[key] = VersionedValue{
		version: args.Version + 1,
		value:   args.Value,
	}

	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
