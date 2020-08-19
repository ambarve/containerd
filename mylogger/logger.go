package mylogger

// A temporary logger that I use for logging stuff during development
// I prefer this over WPA because using WPA/ETL files is much slower than
// just `cat`ing the output of the log file.

import (
	"encoding/json"
	"fmt"
	"os"
)

var (
	logFile *os.File
)

func init() {
	logFile, _ = os.OpenFile("C:\\debuglog.txt",
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
}

func LogFmt(fmtStr string, a ...interface{}) {
	logFile.WriteString(fmt.Sprintf(fmtStr, a...))
}

func LogStruct(data interface{}) {
	jst, _ := json.Marshal(data)
	logFile.WriteString(fmt.Sprintf("%s\n", string(jst)))
}
