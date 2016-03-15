package raft

import (
	"labrpc"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}

func exclude(peers []*labrpc.ClientEnd, me int) (others []int) {
	others = make([]int, 0)
	for i, _ := range peers {
		if i != me {
			others = append(others, i)
		}
	}
	return
}
