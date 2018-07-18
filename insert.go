package main

import (
	"fmt"
	//"io/ioutil"
	"os"
	//"strings"
	"math/rand"
	"bufio"
	"regexp"
	"time"
	"database/sql"
	_ "github.com/lib/pq"
)

type InsertData struct {
	data []string
}

var insertData = [182]InsertData{};

func Insert(src string) error {
	connStr := "postgres://postgres:root@localhost/irise_opti?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err;
	}
	fmt.Println("CONNECTED TO DB !");
	
	file, err := os.Open(src);
	if err != nil {
		fmt.Println("ERROR READ FILE : ");
		fmt.Println(err);
		return err;
	}

	fmt.Println("OPEN FILE DONE !");

	defer file.Close();

	pattern_split := regexp.MustCompile("^--*");
	index := -1;
	start := false;
	scanner := bufio.NewScanner(file);

	for scanner.Scan() {
		line := scanner.Text();

		split := pattern_split.FindStringSubmatch(line);

		if len(split) > 0 {
			start = !start;
			if start == true {
				index = index+1;
			}
		} else {
			if len(line) > 0 {
				insertData[index].data = append(insertData[index].data, line);
			}
		}

	}
	fmt.Println("PARSE FILE DONE !");

	var durations = []time.Duration{};

	for j := 0; j < 10; j++ {
		for i := 0; i < 182; i++ {
			timeStart := time.Now();
			_, err := db.Exec(insertData[i].data[j]);
			if err != nil {
				fmt.Println("x");
			} else {
				fmt.Print(".");
			}
			timeEnd := time.Now();
			durations = append(durations, timeEnd.Sub(timeStart));
			// WAIT
			t := rand.Intn(100);
			time.Sleep(time.Duration(t) * time.Millisecond);
		}
		// WAIT
		t := rand.Intn(100);
		time.Sleep(time.Duration(t) * time.Millisecond);
	}	

	fmt.Println("\n");
	fmt.Println(durations);
	return nil;
}

func main() {
	fmt.Println("STARTING Insert !");

	
	timeStart := time.Now();
	err := Insert("output/parsed/insert_after/arch_a/insert_after.sql");
	if err != nil {
		fmt.Println(err);
	}
	timeEnd := time.Now();
	fmt.Print("PARSING end : ");
	fmt.Println(timeEnd.Sub(timeStart));
	fmt.Println("PARSING files : DONE");

	fmt.Println("ENDING Insert !");
}
