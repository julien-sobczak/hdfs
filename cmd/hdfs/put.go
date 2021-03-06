package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

func put(args []string) {
	if len(args) != 2 {
		printHelp()
	}

	if args[0] == "-" { // Read from Stdin
		
		dests, nn, err := normalizePaths(args[1:])
		if err != nil {
			fatal(err)
		}
		
		client, err := getClient(nn)
		if err != nil {
			fatal(err)
		}
	
		writer, err := client.Create(dests[0])
		if err != nil {
			fatal(err)
		}
	
		defer writer.Close()
		_, err = io.Copy(writer, os.Stdin)
		if err != nil {
			fatal(err)
		}
		
		
	} else { // Read from given input source
		
		dests, nn, err := normalizePaths(args[1:])
		if err != nil {
			fatal(err)
		}

		dest := dests[0]
		source, err := filepath.Abs(args[0])
		if err != nil {
			fatal(err)
		}

	
		client, err := getClient(nn)
		if err != nil {
			fatal(err)
		}
	
		// If the destination is an existing directory, place it inside. Otherwise,
		// the destination is really the parent directory, and we need to rename the
		// source directory as we copy.
		existing, err := client.Stat(dest)
		if err == nil {
			if existing.IsDir() {
				dest = path.Join(dest, filepath.Base(source))
			} else {
				fatal(&os.PathError{"mkdir", dest, os.ErrExist})
			}
		} else if !os.IsNotExist(err) {
			fatal(err)
		}
	
		var err_walking error = nil
		mode := 0755 | os.ModeDir
		err = filepath.Walk(source, func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				err_walking = err
				return nil
			}
	
			rel, err := filepath.Rel(source, p)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				err_walking = err
				return nil
			}
	
			fullDest := path.Join(dest, rel)
			if fi.IsDir() {
				client.Mkdir(fullDest, mode)
			} else {
				reader, err := os.Open(p)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					err_walking = err
					return nil
				}
				defer reader.Close()

				writer, err := client.Create(fullDest)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					err_walking = err
					return nil
				}
				defer writer.Close()
				
				_, err = io.Copy(writer, reader)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					err_walking = err
				}
			}
	
			return nil
		})
		if err_walking != nil {
			os.Exit(1)
		}
	}
}
