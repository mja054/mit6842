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
	view    View
	KVSERVPingTimer map[string]int
	Backup  string
	waiting bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	{
		if args.Viewnum == 0 {
			if vs.view.Primary == args.Me {
				log.Println("Primary (%s) restarted.",
					args.Me)
				vs.view.Primary = ""
			} else if vs.view.Backup == args.Me {
				log.Println("Backup (%s) restarted.",
					args.Me)
				vs.view.Backup = ""
			}
		}
		if (vs.view.Primary == "") && (vs.view.Backup != args.Me) {
			log.Printf("Setting Primar %s\n", args.Me);
			vs.view.Primary = args.Me
			log.Printf("Changing the Viewnum %d->%d\n",
				vs.view.Viewnum, vs.view.Viewnum + 1)
			vs.view.Viewnum += 1
		} else if (vs.view.Backup == "") && (args.Me != vs.view.Primary) {
			if vs.waiting == true {
				vs.Backup = args.Me
			} else {
				log.Printf("Setting Backup %s\n", args.Me);
				vs.view.Backup = args.Me
				log.Printf("Changing the Viewnum %d->%d\n",
					vs.view.Viewnum, vs.view.Viewnum + 1)
				vs.view.Viewnum += 1
			}
		}

		if vs.view.Primary == args.Me {
			if vs.waiting == true {
				vs.waiting = false
				if vs.Backup != "" {
					log.Printf("Setting Backup %s\n", vs.Backup)
					log.Printf("Changing the Viewnum %d->%d\n",
						vs.view.Viewnum, vs.view.Viewnum + 1)
					vs.view.Backup = vs.Backup
					vs.Backup = ""
					vs.view.Viewnum += 1
				}
			} else {
				vs.waiting = true
			}
		}
		vs.KVSERVPingTimer[args.Me] = 0
		reply.View.Viewnum = vs.view.Viewnum
		reply.View.Primary = vs.view.Primary
		reply.View.Backup  = vs.view.Backup
	}
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
		reply.View.Viewnum = vs.view.Viewnum
		reply.View.Primary = vs.view.Primary
		reply.View.Backup  = vs.view.Backup
	}
	vs.mu.Unlock()
	return nil
}


func (vs *ViewServer) _find_KVServ() string {
	for k, _ := range vs.KVSERVPingTimer {
		if k == vs.view.Primary ||
			k == vs.view.Backup ||
			k == vs.Backup {
			continue
		}
		return k
	}
	return ""
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	kvserv := ""
	vs.mu.Lock()
	for key, _ := range vs.KVSERVPingTimer {
		vs.KVSERVPingTimer[key] += 1
		if vs.KVSERVPingTimer[key] == DeadPings {
			delete(vs.KVSERVPingTimer, key)
			if vs.view.Primary == key {
				log.Printf("Primary %s timed out\n",
					vs.view.Primary);
				vs.view.Primary = ""
			} else if vs.view.Backup == key {
				log.Printf("Backup %s timed out\n",
					vs.view.Backup);
				vs.view.Backup = ""
			}
		}
	}
	if vs.view.Primary == "" {
		increment := false
		if vs.view.Backup != "" {
			log.Printf("Setting Backup to Primary %s\n",
				vs.view.Backup);
			increment = true
			vs.view.Primary = vs.view.Backup
			kvserv = vs._find_KVServ()
			if (kvserv == "") {
				log.Printf("No server found to assign backkup\n")
				vs.view.Backup = ""
			} else {
				log.Printf("Setting Backup to %s\n", kvserv)
				vs.view.Backup = kvserv
			}
		} else {
			kvserv = vs._find_KVServ()
			if (kvserv != "") {
				increment = true
				log.Printf("Setting Primary %s\n", kvserv);
				vs.view.Primary = kvserv
				vs.view.Backup = vs._find_KVServ()
			} else {
				log.Printf("No server found to assign to Primary.\n")
			}
		}
		if (increment) {
			log.Printf("Changing the Viewnum %d->%d\n",
				vs.view.Viewnum, vs.view.Viewnum + 1)
			vs.view.Viewnum += 1
		}
	} else if vs.view.Backup == "" {
		if vs.waiting == true {
			kvserv = vs._find_KVServ()
			if (kvserv != "") {
				vs.Backup = kvserv
				log.Println("Currently waiting for primary to reply, registering backup %s",
					kvserv);
			}
		} else {
			kvserv = vs._find_KVServ()
			if (kvserv != "") {
				log.Printf("Setting Backup %s\n", kvserv);
				vs.view.Backup = kvserv
				log.Printf("Changing the Viewnum %d->%d\n",
					vs.view.Viewnum, vs.view.Viewnum + 1)
				vs.view.Viewnum += 1
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

	vs.view.Viewnum = 0
	vs.view.Primary = ""
	vs.view.Backup = ""
	
	vs.KVSERVPingTimer = make(map[string]int)
	vs.Backup = ""
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
