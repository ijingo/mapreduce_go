package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

const debugEnable = false

func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnable {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase jobPhase = "Reduce"
)

type KeyValue struct {
	Key   string
	Value string
}

func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func doMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(file string, contents string) []KeyValue,
) {

	buff, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("Mapper read fail ", err)
	}

	res := mapF(inFile, string(buff))

	for i := 0; i < nReduce; i++ {
		outFileName := reduceName(jobName, mapTaskNumber, i)
		outFile, err := os.Create(outFileName)
		defer outFile.Close()
		if err != nil {
			log.Fatal("Mapper create itermediate file fail ", err)
		}
		enc := json.NewEncoder(outFile)
		for _, kv := range res {
			if ihash(kv.Key)%uint32(nReduce) == uint32(i) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("Encode error ", err)
				}
			}
		}
	}
}

func doReduce(
	jobName string,
	reduceTaskNumber int,
	nMap int,
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inFileName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.Open(inFileName)
		defer inFile.Close()
		if err != nil {
			log.Fatal("Reducer open file fail ", err)
		}

		dec := json.NewDecoder(inFile)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			if _, ok := kvs[kv.Key]; !ok {
				kvs[kv.Key] = make([]string, 0)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	var keys []string
	for k, _ := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outFileName := mergeName(jobName, reduceTaskNumber)
	outFile, err := os.Create(outFileName)
	defer outFile.Close()
	if err != nil {
		log.Fatal("Reducer create file fail ", err)
	}
	enc := json.NewEncoder(outFile)
	for _, k := range keys {
		res := reduceF(k, kvs[k])
		enc.Encode(&KeyValue{k, res})
	}
}
