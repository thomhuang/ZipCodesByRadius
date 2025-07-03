package main

import (
	"os"
	"strings"

	"github.com/dhconnelly/rtreego"
)

func OutputLogFile() {
	if logger.Length > 0 {
		logFile, _ := os.Create("./NearbyZipCodesLog.txt")
		defer logFile.Close()

		_, _ = logFile.Write([]byte(strings.Join(logger.Records, "\n")))
	}
}

func (l *Logger) Append(message string) {
	l.Length++
	l.Records = append(l.Records, message)
}

func (p *PostalCodeItem) Bounds() rtreego.Rect {
	return *p.Rect
}
