package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"bufio"
	"regexp"
)

func Parser(src, target string) error {
	files, err := ioutil.ReadDir(src)
    if err != nil {
		fmt.Println("ERROR READ DIR : ");
		fmt.Println(err);
		return err
    }

    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err
    }

    for _, file := range files {

		fmt.Printf("file : "+file.Name()+"\n");

		path := filepath.Join(target, file.Name());
		fmt.Printf("TEST : "+path+"\n");

        if file.IsDir() {
			Parser(src + file.Name() +"/", target);
            continue
		}

		if strings.HasSuffix(file.Name(), ".zip") {
			continue
		}

		if strings.HasSuffix(file.Name(), ".txt") {
			ParseFile(src + file.Name(), target);
			continue
		}
		
        
    }

    return nil
}

func ParseFile(src, target string) error {
	file, err := os.Open(src);
	if err != nil {
		fmt.Println("ERROR READ FILE : ");
		fmt.Println(err);
		return err
	}
	
	defer file.Close();

	scanner := bufio.NewScanner(file);

	pattern_device := regexp.MustCompile("^APPLIANCE\\s:\\s([a-zA-Z0-9\\s\\(\\),]*)");
	pattern_home := regexp.MustCompile("^HOUSEHOLD\\s:\\s([0-9]*)");
	pattern_line := regexp.MustCompile("^(([0-9]{2}\\/?){3})\\t(([0-9]{2}:?){2})\\t([0-9]*)\\t([0-9]*)");

	output := "";
	for scanner.Scan() {
		line := scanner.Text();

		results_device := pattern_device.FindStringSubmatch(line);
		results_home := pattern_device.FindStringSubmatch(line);
		results_line := pattern_device.FindStringSubmatch(line);
		if len(results_device) > 0 {
			deviceId := results_device[1];
		}
		if len(results_home) > 0 {
			homeId := results_home[1];
		}
		if len(results_line) > 0 {

		}
	}

	fmt.Println(output);

	return nil;
}



func main() {
	fmt.Println("STARTING Parser !");
	if err := os.RemoveAll("output/parsed/"); err != nil {
		fmt.Println("ERROR CLEAR DIST : ");
		fmt.Println(err);
    }
	fmt.Println("CLEARING output : DONE");

	Parser("output/unziped/", "output/parsed/");
	fmt.Println("PARSING files : DONE");

	fmt.Println("TODO : CLEAN ZIPED FILES");

	fmt.Println("ENDING Parser !");
}
