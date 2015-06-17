package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"regexp"
	"strings"
	"sync"
)

func main() {
	file := flag.String("import", "~/redis.csv", "a csv file")
	expFile := flag.String("export", "~/misses.json", "path to export file")
	existingFile := flag.String("existing", "~/fromS3.txt", "path to s3 dump")

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

	exists := getKeys(*existingFile)

	hits := make([]string, 0)
	misses := make([]string, 0)

	stats := func() {
		h := len(hits)
		m := len(misses)
		t := h + m
		r := float64(h) / float64(t)
		fmt.Printf("hits: %d  misses: %d  total: %d  hit-ratio: %f\n", h, m, t, r)
	}

	// sanity check, display to standard output
	for _, lines := range rawCSVdata {
		for i, line := range lines {
			uid := strings.Split(line, ":")[1]

			if _, hit := exists[uid]; hit {
				hits = append(hits, uid)
				//	fmt.Printf("%s hit\n", uid)
			} else {
				misses = append(misses, uid)
				//	fmt.Printf("%s miss\n", uid)
			}
			if i%100 == 0 {
				stats()
			}
		}
	}

	fmt.Println("Finished -------------------------------------------")
	stats()

	jsondata, err := json.Marshal(misses)
	jsonFile, err := os.Create(*expFile)

	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	jsonFile.Write(jsondata)
	jsonFile.Close()
}

const s3Root = "s3-us-west-2.amazonaws.com"
const bucketName = "images.takuapp.com"

func getKeys(path string) map[string]bool {
	keys := make(map[string]bool)
	lines, err := readLines(path)
	if err != nil {
		fmt.Println(err.Error())
		return keys
	}
	var uidRegex = regexp.MustCompile(`s3:\/\/images.takuapp.com\/(.*$)`)
	for _, line := range lines {
		parts := uidRegex.FindStringSubmatch(line)
		if len(parts) > 1 {
			keys[parts[1]] = true
		}
	}
	return keys
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

//func get(path string) (bool, error) {
//	//we get keys everytime because they can expire... this could be improved
//	keys, err := s3gof3r.InstanceKeys() // get S3 keys
//	if err != nil {
//		return false, err
//	}
//
//	s3 := s3gof3r.New(s3Root, keys)
//
//	b := s3.Bucket(bucketName)
//
//	rc, _, err := b.GetReader(path, nil)
//	if err != nil {
//		return false, err
//	}
//	rc.Close()
//	//buffer := make([]byte, 100)
//	//_, err = rc.Read(buffer)
//	//if err != nil {
//	//	return false, err
//	//}
//
//	return true, nil
//}
var wg sync.WaitGroup

func getKeyss3() map[string]bool {
	d := downloader{bucket: bucketName, keys: make(map[string]bool)}

	bn := bucketName
	prefix := ""
	client := s3.New(nil)
	wg.Add(1)
	params := &s3.ListObjectsInput{Bucket: &bn, Prefix: &prefix}

	client.ListObjectsPages(params, d.eachPage)
	wg.Wait()
	return d.keys
}

type downloader struct {
	bucket, dir string
	keys        map[string]bool
}

func (d *downloader) eachPage(page *s3.ListObjectsOutput, lastPage bool) bool {
	for _, obj := range page.Contents {
		d.keys[*obj.Key] = true
	}
	fmt.Println("page")
	if lastPage {
		wg.Done()
	}
	return true
}
