package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	log.Printf("doReduce: 'Jobname=%s' Called with 'nMap=%d' and  'reduceTaskNumber=%d'", jobName, nMap, reduceTaskNumber)
	// Create / Open MergeFile
	mergePath := mergeName(jobName, reduceTaskNumber)
	log.Printf("Mergepath is %s", mergePath)
	mergeFile, err := os.Create(mergePath)
	if err != nil {
		log.Fatal("Err making MergeFile", err)
	}

	// a map for collecting the key/[]values
	collection := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		fileName := reduceName(jobName, m, reduceTaskNumber)
		log.Printf("Opening reduceFile: %s", fileName)
		reduceFile, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(reduceFile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			collection[kv.Key] = append(collection[kv.Key], kv.Value)
		}

		log.Println("M called:", m)
	}
	enc := json.NewEncoder(mergeFile)
	for k, v := range collection {
		enc.Encode(KeyValue{k, reduceF(k, v)})
		// log.Println(k, collection[k])
	}
	log.Printf("Collection is %d long", len(collection))
	mergeFile.Close()
}
