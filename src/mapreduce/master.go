package mapreduce

import (
	"container/list"
	"fmt"
	"time"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
	jobID   int
	isIdle  bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func initWorkerInfo(workerAddress string) *WorkerInfo {
	wInfo := new(WorkerInfo)
	wInfo.address = workerAddress
	wInfo.jobID = -1
	wInfo.isIdle = true

	return wInfo
}

func (mr *MapReduce) waitForJobCompletion() {
	for {
		<-mr.workerChan
		if mr.jobsInProgreess == 0 {
			fmt.Println("All reduce jobs completed")
			mr.workerChan <- 1
			break
		}
		mr.workerChan <- 1
		time.Sleep(1000 * time.Millisecond)
	} // All reduce jobs are complete
}

func (mr *MapReduce) callWorkerDoJob(worker string, 
	jobType JobType, jobID int, n int) {
	var reply DoJobReply
	args := &DoJobArgs{}
	args.File = mr.file
	args.Operation = jobType
	args.JobNumber = jobID
	args.NumOtherPhase = n

	fmt.Printf("%s: Assigning job %d to %s worker\n",
		jobType, jobID, worker);
	ok := call(worker, "Worker.DoJob",
		args, &reply)
	if ok == false {
		fmt.Printf("%s: DoJob %s error\n",
			jobType, worker)
	}
	fmt.Printf("%s: Job %d completed by %s worker\n",
		jobType, jobID, worker)
	//mr.Workers[worker].jobID = jobID
	<-mr.workerChan
	mr.Workers[worker].isIdle = true
	mr.idleWorkerCnt += 1
	mr.jobsInProgreess -= 1	
	mr.workerChan <- 1
}

func (mr *MapReduce) handleWorkerRegistration() {
	for {
		worker := <- mr.registerChannel
		fmt.Printf("New %s worker registered\n", worker);
		mr.Workers[worker] = initWorkerInfo(worker)
		<-mr.workerChan
		{
			mr.idleWorkerCnt += 1
		}
		mr.workerChan <- 1
	}
}
func (mr *MapReduce) _findIdleWorker() string {
	var mWorker string
	for _, worker := range mr.Workers {
		if worker.isIdle == true {
			mWorker = worker.address

			worker.isIdle = false

			mr.idleWorkerCnt -= 1
			mr.jobsInProgreess += 1
			break
		}
	}
	return mWorker
}

func (mr *MapReduce) findIdleWorker() string {
	var mWorker string
	for {
		time.Sleep(1 * time.Millisecond)
		<-mr.workerChan
		if mr.idleWorkerCnt == 0 {
			mr.workerChan <- 1
			continue
		}
		mWorker = mr._findIdleWorker()
		mr.workerChan <- 1
		break
	}
	return mWorker
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	fmt.Println("RunMaster started")

	go mr.handleWorkerRegistration()

	fmt.Println("starting Map process")

	nMap := mr.nMap
	for nMap > 0 {
		mWorker := mr.findIdleWorker()
		go mr.callWorkerDoJob(mWorker, Map, 
			nMap - 1, mr.nReduce);

		nMap -= 1
	}
	fmt.Println("Assignment complete, waiting for completion")
	mr.waitForJobCompletion()

	// Begining of Reduce
	fmt.Println("RunMaster end")

	nReduce := mr.nReduce
	for nReduce > 0 {
		mWorker := mr.findIdleWorker()
		go mr.callWorkerDoJob(mWorker, Reduce, 
			nReduce - 1, mr.nMap);
		nReduce -= 1
	}
	fmt.Println("Assignment complete, waiting for completion")
	mr.waitForJobCompletion()
	return mr.KillWorkers()
}
