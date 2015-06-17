package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
)

func main() {
	file := flag.String("import", "redis.csv", "a csv file")
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
			fmt.Println(line)
		}
	}
}
