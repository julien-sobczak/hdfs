package main

/*
 * A rewrite of the ls command to:
 * - use the output of the hadoop fs -ls command
 * - support the -R option to traverse the file system recursively
 */
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


type LsOptions struct {
	directoryAsPlainFile bool 	// Option '-d': Directories are listed as plain files.
    humanReadable bool 			// Option '-h: Format file sizes in a human-readable fashion (eg 64.0m instead of 67108864).
	recursive bool 				// Option '-R': Recursively list subdirectories encountered.
}


func ls2(paths []string, options *LsOptions) {
	original_paths := paths
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		if len(original_paths) > 0 {
			fmt.Fprintf(os.Stderr, "ls: '%s': No such file or directory\n", original_paths[0])
		} else {
			fmt.Fprintf(os.Stderr, "ls: No such file or directory\n")
		}
		os.Exit(1)
	}

	tw := lsrTabWriter()
	for _, p := range paths {
	    traversePath(tw, client, p, options, true)
	}
	tw.Flush()
}

func traversePath(tw *tabwriter.Writer, client *hdfs.Client, name string, options *LsOptions, root bool) {

	file, err := client.Stat(name)
	if err != nil {
		fatal(err)
	}


    if file.IsDir() {
    	if !root||options.directoryAsPlainFile {
    		showStatus(tw, name, file, options)
    		tw.Flush()
    	}
    	if !options.directoryAsPlainFile&&(root||options.recursive) {
			dirReader, err := client.Open(name)
			if err != nil {
				fatal(err)
			}
	
			// recurse over children
			var partial []os.FileInfo
			for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
				if err != nil {
					fatal(err)
				}
				traversePaths(tw, client, name, partial, options, false)
			}
		}
	} else {
		showStatus(tw, name, file, options)
	}
}

func traversePaths(tw *tabwriter.Writer, client *hdfs.Client, parent string, files []os.FileInfo, options *LsOptions, root bool) {
	for _, file := range files {
		if strings.HasPrefix(file.Name(), ".") {
			continue
		}

		traversePath(tw, client, path.Join(parent, file.Name()), options, root)
		tw.Flush()
	}
}

func showStatus(tw *tabwriter.Writer, name string, info os.FileInfo, options *LsOptions) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	if options.humanReadable {
		size = formatBytes(fi.Size())
	}
	replication := strconv.FormatUint(uint64(fi.BlockReplication()), 10)

	modtime := fi.ModTime()
	date := modtime.Format("2006-01-02")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	fmt.Fprintf(tw, "%s \t%s \t%s \t %s \t %13s \t%s \t%s \t%s\n",
		mode, replication, owner, group, size, date, timeOrYear, name)
}

func ls2TabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}

