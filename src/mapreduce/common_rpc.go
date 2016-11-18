package mapreduce

import (
	"fmt"
	"net/rpc"
)

type DoTaskArgs struct {
	JobName       string
	File          string
	Phase         jobPhase
	TaskNumber    int
	NumOtherPhase int
}

type ShutdownReply struct {
	Ntasks int
}

type RegisterArgs struct {
	Worker string
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
