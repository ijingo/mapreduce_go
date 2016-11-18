package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

type Master struct {
	sync.Mutex

	address         string
	registerChannel chan string
	doneChannel     chan bool
	workers         []string // protected by the mutex

	// per-task info
	jobName string
	files   []string
	nReduce int

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// a RPC method
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)
	go func() {
		mr.registerChannel <- args.Worker
	}()
	return nil
}

// a RPC method
func (mr *Master) Shutdown(_, _ *struct{}) error {
	debug("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.l.Close()
	return nil
}

func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.address)
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Fatal("RegistrationServer", mr.address, " error: ", e)
	}
	mr.l = l
	go func() {
	LOOP:
		for {
			select {
			case <-mr.shutdown:
				break LOOP
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				debug("RegistrationServer: accpet errror ", err)
			}
		}
		debug("RegistrationServer: done\n")
	}()
}

func (mr *Master) stopRPCServer() {
	ok := call(mr.address, "Master.Shutdown", new(struct{}), new(struct{}))
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
	debug("cleanupRegsitration: doen\n")
}

func (mr *Master) merge() {
	debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		defer file.Close()
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create("mrtmp." + mr.jobName)
	defer file.Close()
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
}

func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

func (mr *Master) CleanupFiles() {
	for i := range mr.files {
		for j := 0; j < mr.nReduce; j++ {
			removeFile(reduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		removeFile(mergeName(mr.jobName, i))
	}
	removeFile("mrtmp." + mr.jobName)
}

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(tasksNumber int, nios int, phase jobPhase) {
			defer wg.Done()
			for {
				worker := <-mr.registerChannel
				args := DoTaskArgs{mr.jobName, mr.files[tasksNumber], phase, tasksNumber, nios}
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						mr.registerChannel <- worker
					}()
					break
				}
			}
		}(i, nios, phase)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) Wait() {
	<-mr.doneChannel
}

func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	nTasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			nTasks = append(nTasks, reply.Ntasks)
		}
	}
	return nTasks
}

func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.registerChannel = make(chan string)
	mr.doneChannel = make(chan bool)
	return mr
}

func (mr *Master) run(jobName string,
	files []string,
	nReduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nReduce
	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

func Sequential(jobName string, files []string, nReduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nReduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nReduce}
	})
	return
}

func Distributed(jobName string, files []string, nReduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nReduce, mr.schedule, func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	})
	return
}
