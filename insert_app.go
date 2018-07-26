package main

import (
	"fmt"
	//"io/ioutil"
	"os"
	"strings"
	"math/rand"
	"bufio"
	"regexp"
	"time"
	"database/sql"
	_ "github.com/lib/pq"
    "strconv"
)

type InsertData struct {
	id string
	values []int
	data []string
}

type MyDB struct {
    *sql.DB
}

var insertData = [182]InsertData{};
var durations = []time.Duration{};

func Insert(src string) error {
	connStr := "postgres://Golang:azerty@localhost/irise_opti?sslmode=disable"
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
				id := line[2:len(line)]
				insertData[index].id = id;
			}
		} else {
			if len(line) > 0 {
				data := strings.Split(line, "--");
				insertData[index].data = append(insertData[index].data, data[0]);
				value, err := strconv.Atoi(data[1])
				if err != nil {
					return err;
				}
				insertData[index].values = append(insertData[index].values, value);
			}
		}

	}
	fmt.Println("PARSE FILE DONE !");

	for j := 0; j < 10; j++ {
		for i := 0; i < 182; i++ {
			go InsertConcurrence(db, insertData[i].id, insertData[i].values[j], insertData[i].data[j]);
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

func InsertConcurrence(db *sql.DB, id string, _value int, request string) {

	rows, err := db.Query("SELECT value FROM Telemetry WHERE appliance_id = '" + id + "' ORDER BY datetime DESC LIMIT 1")
	if err != nil {
		fmt.Print("S");
	} else {
		defer rows.Close()
		for rows.Next() {
			var value int
			if err := rows.Scan(&value); err != nil {
				fmt.Print("V");
			} else {
				if (_value == value) {
					fmt.Print("-");
				} else {
					timeStart := time.Now();
					_, err := db.Exec(request);
					if err != nil {
						fmt.Print("x");
					} else {
						fmt.Print(".");
					}
					timeEnd := time.Now();
					durations = append(durations, timeEnd.Sub(timeStart));
				}
			}
		}
	}
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
