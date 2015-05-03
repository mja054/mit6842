package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	primary string
	backup  string
	viewNum uint
	kvserv  map[string]int
	waiting bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	log.Printf("Ping from %s server with viewNum %d\n", args.Me, args.Viewnum)

	vs.mu.Lock()
	if vs.waiting {
		if (vs.primary == "") {
			log.Fatalf("Primary can never be empty when we are in waiting state\n")
		} else if (vs.primary == args.Me) {
			if (args.Viewnum == 0) {
				log.Printf("Primary restarted\n")
				if (vs.backup != "") {
					vs.primary = vs.backup
					log.Printf("New Primary %s\n", vs.primary)
					vs.viewNum += 1
					log.Printf("viewNum = %d\n", vs.viewNum)
					vs.backup = args.Me
					log.Printf("New Backup %s\n", vs.backup)
					log.Printf("Coming out of wait state\n")
					vs.waiting = false
				} else {
					log.Printf("Retaining the primary %s and in waiting state\n", vs.primary)
					// We should enter waiting state again
				}
			} else {
				vs.waiting = false
				log.Printf("Coming out of wait state\n")
				if (vs.backup == "") {
					vs.backup = vs.get_server()
					if (vs.backup != "") {
						log.Printf("New backup server %s\n", vs.backup)
						vs.viewNum += 1;
						log.Printf("viewNum = %d\n", vs.viewNum)
					}
				}
			}
		}
	} else {
		if (vs.primary == "") {
			vs.primary = args.Me
			log.Printf("New primary %s\n", vs.primary)
			vs.waiting = true
			log.Printf("Entering wait state, primary %s\n", vs.primary)
			vs.viewNum += 1;
			log.Printf("viewnum = %d\n", vs.viewNum)
		} else if (args.Me == vs.primary) {
			if (args.Viewnum == 0) {
				log.Printf("Primary %s restarted\n", vs.primary)
				if (vs.backup == "") {
					log.Printf("No backup, Entering wait state, primary %s\n", vs.primary)
					vs.waiting = true
				} else {
					vs.primary = vs.backup
					log.Printf("New primary %s\n", vs.primary)
					vs.backup = args.Me
					log.Printf("New backup %s\n", vs.backup)
					vs.viewNum += 1
					log.Printf("viewnum = %d\n", vs.viewNum)
				}
			} else {
				if (args.Viewnum == vs.viewNum) {
					log.Printf("Same view num, not entering wait state\n");
				} else {
					vs.waiting = true
					log.Printf("Different view num, entering wait state, primary %s\n", vs.primary)
				}
			}
		} else if (vs.backup == "") {
			vs.backup = args.Me
			log.Printf("New backup %s\n", vs.backup)
			vs.viewNum += 1
			log.Printf("viewNum = %d\n", vs.viewNum)
		}
	}
	vs.kvserv[args.Me] = 0

	reply.View.Viewnum = vs.viewNum
	reply.View.Primary = vs.primary
	reply.View.Backup  = vs.backup

	vs.mu.Unlock();
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	{
		reply.View.Viewnum = vs.viewNum
		reply.View.Primary = vs.primary
		reply.View.Backup  = vs.backup
	}
	vs.mu.Unlock()
	return nil
}


func (vs *ViewServer) get_server() string {
	for k, _ := range vs.kvserv {
		if k == vs.primary ||
			k == vs.backup {
			continue
		}
		log.Printf("returning server %s\n", k)
		return k
	}
	log.Printf("Could not find the server\n")

	return ""
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	for key, _ := range vs.kvserv {
		vs.kvserv[key] += 1
		if vs.kvserv[key] == DeadPings {
			delete(vs.kvserv, key)
			if vs.primary == key {
				log.Printf("Primary %s timed out\n",
					vs.primary);
				if vs.waiting {
					vs.kvserv[key] = 0
					log.Printf("In waiting state, so cannot remove primary %s\n",
						vs.primary)
				} else {
					vs.primary = ""
					if (vs.backup != "") {
						vs.primary = vs.backup
						log.Printf("New primary %s\n",
							vs.primary);
						vs.viewNum += 1
						vs.backup = vs.get_server()
					} else {
						vs.primary = vs.get_server()
						if (vs.primary != "") {
							vs.viewNum += 1
						}
					}
				}
			} else if (key == vs.backup) {
				vs.backup = vs.get_server()
				if (vs.backup != "") {
					log.Printf("New backup %s\n",
						vs.backup);
					vs.viewNum += 1
				}
			} else {
				log.Printf("%s server timed out\n", key);
			}
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
//	vs.mu = &sync.Mutex{}

	vs.viewNum = 0
	vs.primary = ""
	vs.backup = ""
	
	vs.kvserv = make(map[string]int)
	vs.waiting = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				log.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
