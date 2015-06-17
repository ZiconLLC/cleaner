package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/rlmcpherson/s3gof3r"
	"os"
	"strings"
)

func main() {
	file := flag.String("import", "redis.csv", "a csv file")
	flag.Parse()
	csvfile, err := os.Open(*file)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer csvfile.Close()

	reader := csv.NewReader(csvfile)

	reader.FieldsPerRecord = -1 // see the Reader struct information below

	rawCSVdata, err := reader.ReadAll()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// sanity check, display to standard output
	for _, lines := range rawCSVdata {
		for _, line := range lines {
			uid := strings.Split(line, ":")[1]
			if hit, _ := get(uid); hit {
				fmt.Printf("%s hit\n", uid)
			} else {
				fmt.Printf("%s miss\n", uid)
			}
		}
	}
}

const s3Root = "s3-us-west-2.amazonaws.com"
const bucketName = "images.takuapp.com"

func get(path string) (bool, error) {
	//we get keys everytime because they can expire... this could be improved
	keys, err := s3gof3r.InstanceKeys() // get S3 keys
	if err != nil {
		return false, err
	}

	s3 := s3gof3r.New(s3Root, keys)

	b := s3.Bucket(bucketName)

	rc, _, err := b.GetReader(path, nil)
	if err != nil {
		return false, err
	}
	defer rc.Close()
	buffer := make([]byte, 512)
	_, err = rc.Read(buffer)
	if err != nil {
		return false, err
	}

	return true, nil
}
