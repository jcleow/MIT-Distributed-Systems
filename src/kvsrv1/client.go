package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	// Perpetually retrying the network call if it fails
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}

		// Only exit if the network call is successful or if the key does not exist
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrNoKey {
				return "", 0, rpc.ErrNoKey
			}

			return reply.Value, reply.Version, reply.Err
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.
//
// If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server.
//
// If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	count := 0
	for {
		args := rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {

			// Put should return ErrVersion since Put was not performed at the server in the first instance
			if reply.Err == rpc.ErrVersion {
				if count == 0 {
					return reply.Err
				}
				// but if we retried the PUT and it errored with ErrVersion:
				// As per the https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html notes:
				// the first RPC might have been executed by the server
				// but the network may have discarded the successful response from the server,
				// so that the server sent rpc.ErrVersion only for the retransmitted RPC.

				// Or, it might be that another Clerk updated the key before the
				// Clerk's first RPC arrived at the server, so that the server executed neither of the
				// Clerk's RPCs and replied rpc.ErrVersion to both.

				// Return ErrMaybe to the application since its earlier RPC might have been
				// processed by the server successfully but the response was lost
				// and Clerk doesn't know if the Put was performed or not

				/*
					Scenario 1: "First RPC executed, reply lost"

					  1. Client sends Put("k", "v", 5) — RPC #1
					  2. Server receives it, executes it, version becomes 6, sends back OK
					  3. Network drops the reply — client never gets it
					  4. Client retries Put("k", "v", 5) — RPC #2
					  5. Server receives it, but version is now 6, not 5 → replies ErrVersion

					  The server sent ErrVersion to RPC #2, not RPC #1. RPC #1 got OK but the client never saw it.

					  Scenario 2: "Another clerk changed it first"

					  1. Client A sends Put("k", "v", 5) — RPC #1
					  2. Network delays RPC #1
					  3. Client B sends Put("k", "x", 5) → succeeds, version becomes 6
					  4. Client A's RPC #1 finally arrives, version is 6 not 5 → server replies ErrVersion
					  5. Network drops that reply too
					  6. Client A retries Put("k", "v", 5) — RPC #2
					  7. Server sees version 6 not 5 → replies ErrVersion

					  Both RPCs got ErrVersion, neither executed.
				*/
				// Meaning to say there is a chance that our initial RPC completed successfully or not, but
				// because the network dropped the RPC, we wouldn't know, hence return ErrMaybe
				return rpc.ErrMaybe
			}
			return reply.Err
		} else {
			count += 1
		}

		// Add sleep before retry
		time.Sleep(100 * time.Millisecond)

	}
}
