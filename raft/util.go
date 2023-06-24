package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = 1
const WriteToDisk = false

var FileName = fmt.Sprintf("debug_raft[%v].log", time.Now().Format("2006-01-02_15-04-05"))

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		if WriteToDisk {
			f, err := os.OpenFile(FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			timestamp := time.Now().Format("2006/01/02 15:04:05")
			text := fmt.Sprintf("%s ", timestamp) + fmt.Sprintf(format, a...)
			if _, err = f.WriteString(text + "\n"); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Printf(format, a...)
		}
	}
	return
}
