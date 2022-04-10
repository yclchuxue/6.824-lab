package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	// "time"

	//"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

// import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var wg sync.WaitGroup

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



func Map(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, args Args, reply Reply) {


		args.File = reply.Index
		args.Index = reply.MapIndex

		kv := mapf(reply.Filename, string(reply.Content))

		//fmt.Println("len(kv) = ", len(kv))
		//fmt.Println("index = ", args.Index, "\t", reply.Index)
		for i := 0; i < len(kv); i++ {
			x := ihash(kv[i].Key) % 10
			name := fmt.Sprintf("mr-%d%d.txt",args.Index, x)

			newfile, err := os.OpenFile(name, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(newfile)

			err = enc.Encode(&kv[i])
			if err != nil {
				log.Fatal(err)
			}

			newfile.Close()
			//fmt.Fprintf(opfiles[x], "%v %v\n", kv[i].Key, kv[i].Value)
			//fmt.Println(kv[i].Key, "\n" , kv[i].Value)
		}
		//fmt.Println("x = ", reply.Index)
		args.Done = true
		ok := call("Coordinator.Mapf", &args, &reply)
		if !ok {
			fmt.Printf("4 call done failed\n")
			//return nil
		}

		//return nil
}

func Reduce(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,args Args, reply Reply) {

		args.File = reply.Index
		args.Index = reply.Index
		//fmt.Println("a = ", args.File)
		mediate := []KeyValue{}
		for _,name := range reply.Filenames {
			// fmt.Println(name)
			file, err := os.OpenFile(name,os.O_CREATE | os.O_RDWR, 0666)
			if err != nil {
				fmt.Println("1 open file failed!")
				//return nil
				// panic(err)
			}
			x := 0
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				x++
				mediate = append(mediate, kv)
			}
			// fmt.Println(x)
			x = 0
			file.Close()
		}

		sort.Sort(ByKey(mediate))
		

		filename := fmt.Sprintf("mr-out-%d.txt", args.File)

		newfile,err := os.OpenFile(filename, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("2 open file failed!")
			//return nil
		}

		i := 0
		for i < len(mediate) {
			j := i + 1
			for j < len(mediate) && mediate[j].Key == mediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, mediate[k].Value)
			}
			output := reducef(mediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(newfile, "%v %v\n", mediate[i].Key, output)

			i = j
		}
		newfile.Close()

		//fmt.Println(filename)

		args.Done = true
		ok := call("Coordinator.Reducef", &args, &reply)
		if !ok {
			fmt.Printf("3 call done failed\n")
		}

		//return nil
}

func taskcall(mapf func(string, string) []KeyValue,
reducef func(string, []string) string, index int)  {

	defer func() {
        if err := recover(); err != nil {
            log.Println("work failed:", err)
        }
    }()
	
	for{
		args := Args{}

		//args.Index = index

		args.Done = false

		reply := Reply{}

		//fmt.Println(index)

		ok := call("Coordinator.Task", &args, &reply)
		if ok {
			// if reply.Work == "" {
			// 	fmt.Println("AAA")
			// }
			//fmt.Println("aaa", reply.Work, reply.Index, reply.Filename)
			if reply.Work == "map" {
				Map(mapf, reducef, args, reply)
			}
			if reply.Work == "reduce" {
				Reduce(mapf, reducef, args, reply)
			}	
			if reply.Work == "other" {
				continue
			}
			if reply.Work == "break" {
				break
			}		
		}else{
			fmt.Println("call failed!")
			//log.Fatalf("call failed!")
		}

		// time.Sleep(time.Second * 4)
	}
	//fmt.Println("AAAAA")
	wg.Done()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	wg.Add(30)

	goroutinenum := 30

	for i := 0; i < goroutinenum; i++ {
		go taskcall(mapf, reducef, i)
	}

	wg.Wait()

	//time.Sleep(time.Second * 100)

		//reducef()
		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9999")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
