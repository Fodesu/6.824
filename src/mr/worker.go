package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type wokerstate int

const (
	Map wokerstate = iota
	Reduce
	Wating
)

type Workman struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	status  wokerstate
	workid  int
	Done    bool
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (worker *Workman) logPrintf(format string, vars ...interface{}) {
	log.Printf("worker %d: "+format, worker.workid, vars)
}

func (worker *Workman) HeartBeatAndGetTask() Task {
	args := Task{
		Workerid: worker.workid,
	}
	reply := Task{}
	
	isSuccess := call("Coordinator.Choose", &args, &reply)
	if worker.workid == -1 {
		worker.workid = reply.Workerid
	}
	if !isSuccess {
		fmt.Println("CALL Coordinator.Choose is error")
	}

	return reply
}

func GetKeyValuesFromFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	path := filename
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("can not open %v", path)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("can not read %v", file)
	}

	return mapf(filename, string(content))
}

func ConverAndWriteKvasToFile(task Task, KeyValues []KeyValue) {
	kvas := make([][]KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range KeyValues {
		index := ihash(kv.Key) % task.NReduce
		// fmt.Println("task : ", task.Workerid, "ihash : ", ihash(kv.Key), " task.NReduce : ", task.NReduce, "ihashed index = ", index)
		kvas[index] = append(kvas[index], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		//tmpfile, err := os.CreateTemp(".", "mrtmp")
		tmpfile, err := ioutil.TempFile(".", "mrtmp")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range kvas[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		newname := fmt.Sprintf("mr-%v-%v", task.Id, i)
		err = os.Rename(tmpfile.Name(), newname)
		if err != nil {
			log.Fatalf("rename failed for : %v", newname)
		}
		tmpfile.Close()
	}
}

func (w *Workman) ReportMapTask(task Task) {
	args := MapTaskJoinArgs{}
	args.WorkerId = w.workid
	args.FileId = task.Id
	reply := MapTaskJoinReply{}

	isSuccess := call("Coordinator.ReportMapTask", &args, &reply)
	if !isSuccess {
		w.logPrintf("ReportMapTask'n call fail\n")
	}
	if reply.Accept {
		w.logPrintf("accept\n")
	} else {
		w.logPrintf("not accept\n")
	}
}

func (w *Workman) doMapTask(task Task) {
	fmt.Printf("will open %v\n", task.FileName)
	KeyValues := GetKeyValuesFromFile(task.FileName, w.mapf)
	ConverAndWriteKvasToFile(task, KeyValues)
	w.ReportMapTask(task)
}

func readKvasFromFile(fileId, reduceId int) []KeyValue {
	filename := fmt.Sprintf("mr-%v-%v", fileId, reduceId)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("can not open file : %v", filename)
	}
	dec := json.NewDecoder(file)
	kvas := make([]KeyValue, 0)
	for {
		var kv KeyValue
		err = dec.Decode(&kv)
		if err != nil {
			break
		}
		kvas = append(kvas, kv)
	}
	file.Close()
	return kvas
}

//! sort []keyValue by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceByKey(kvas []KeyValue, reducef func(string, []string) string, ofile io.Writer) {
	sort.Sort(ByKey(kvas))
	for i := 0; i < len(kvas); {
		j := i + 1
		for ; j < len(kvas); j++ {
			if kvas[i].Key != kvas[j].Key {
				break
			}
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvas[k].Value)
		}
		output := reducef(kvas[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kvas[i].Key, output)
		i = j
	}
}

func (w *Workman) ReportReduceTask(reportId int) {
	args := ReduceTaskJoinArgs{}
	args.FileId = reportId
	args.WorkerId = w.workid

	reply := ReduceTaskJoinReply{}
	call("Coordinator.ReportReduceTask", &args, &reply)

	if reply.Accept {
		w.logPrintf("%v was accepted\n", reportId)
	} else {
		w.logPrintf("%v was not accepted\n", reportId)
	}
}

func (w *Workman) doReduceTask(task Task) {
	//! 得到本Task的
	kvas := make([]KeyValue, 0)
	//! 结果文件名
	realName := fmt.Sprintf("mr-out-%v", task.Id)
	for i := 0; i < task.FileNum; i++ {
		//! 轮询所有的MapId和以task.id结尾的文件并解码所有 keyvalues
		kvas = append(kvas, readKvasFromFile(i, task.Id)...)
	}
	//! 创建临时文件写入结果然后重命名，可以保证写入文件是原子的，如果失败则文件消失，成功则文件保留
	tmpfile, err := ioutil.TempFile(".", "TmpReduce")
	fmt.Printf("worerk id : %d, total kvas count %v in the task.id : %v \n", w.workid, len(kvas), task.Id)
	if err != nil {
		w.logPrintf("can not CreateTemp for %v\n")
	}
	reduceByKey(kvas, w.reducef, tmpfile)

	err = os.Rename(tmpfile.Name(), realName)
	if err != nil {
		fmt.Printf("worker id : %d ,rename tempfile failed for %v\n", w.workid, realName)
	}
	tmpfile.Close()
	w.ReportReduceTask(task.Id)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	worker := Workman{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.workid = -1
	worker.status = Map
	worker.Done = false
	for {
		task := worker.HeartBeatAndGetTask()
		if task.Status == Working {
			switch task.TypefoTask {
			case MapTask:
				worker.doMapTask(task)
			case ReduceTask:
				worker.doReduceTask(task)
			}
		} else if task.Status == Waiting {
			time.Sleep(time.Second * 1)
		} else if task.Status == Done {
			worker.Done = true
			break
		} else {
			panic(fmt.Sprintf("unexcept jobtype %v", task.TypefoTask))
		}
	}
	fmt.Printf("worker %v is stop\n", worker.workid)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	fmt.Println(rpcname)
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		panic(err)
	}

	return true
}
