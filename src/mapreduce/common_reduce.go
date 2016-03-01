package mapreduce

import (
	"encoding/json"
	"io"
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
	// Where the values will end up
	mergePath := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergePath)
	if err != nil {
		//log.Fatal("Err making MergeFile", err)
	}

	// a map for collecting the key/[]values
	collection := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		fileName := reduceName(jobName, m, reduceTaskNumber)
		reduceFile, err := os.Open(fileName)
		if err != nil {
		}
		dec := json.NewDecoder(reduceFile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
			}
			collection[kv.Key] = append(collection[kv.Key], kv.Value)
		}

	}
	enc := json.NewEncoder(mergeFile)
	for k, v := range collection {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}
	mergeFile.Close()
}
