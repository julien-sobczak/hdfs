package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"path"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/colinmarc/hdfs"
)

func lsr(paths []string) {
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		paths = []string{userDir()}
	}

	tw := lsrTabWriter()
	for _, p := range paths {
	    processPath(tw, client, p, true)
	}
	tw.Flush()
}

func processPath(tw *tabwriter.Writer, client *hdfs.Client, name string, root bool) {

	file, err := client.Stat(name)
	if err != nil {
		fatal(err)
	}

    if file.IsDir() {
    	
    	if root == false {
	    	processLong(tw, name, file)
		}

		// recurse over children
		dirReader, err := client.Open(name)
		if err != nil {
			fatal(err)
		}

		var partial []os.FileInfo
		for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
			if err != nil {
				fatal(err)
			}
			processPaths(tw, client, name, partial, false)
		}
	} else {
		processLong(tw, name, file)
	}
}

func processPaths(tw *tabwriter.Writer, client *hdfs.Client, parent string, files []os.FileInfo, root bool) {
	for _, file := range files {
		if strings.HasPrefix(file.Name(), ".") {
			continue
		}

		processPath(tw, client, path.Join(parent, file.Name()), false)
		tw.Flush()
	}
}

func processLong(tw *tabwriter.Writer, name string, info os.FileInfo) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	replication := strconv.FormatUint(uint64(fi.BlockReplication()), 10)

	modtime := fi.ModTime()
	date := modtime.Format("Jan _2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	fmt.Fprintf(tw, "%s \t%s \t%s \t %s \t %s \t%s \t%s \t%s\n",
		mode, replication, owner, group, size, date, timeOrYear, name)
}

func lsrTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}
