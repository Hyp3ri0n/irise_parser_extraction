package main

import (
	"fmt"
	"io/ioutil"
	"os"
	//"path/filepath"
	"strings"
	"bufio"
	"regexp"
	"time"
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

		//path := filepath.Join(target, file.Name());
		//fmt.Printf("TEST : "+path+"\n");

        if file.IsDir() {
			Parser(src + file.Name() +"/", target);
            continue
		}

		if strings.HasSuffix(file.Name(), ".zip") {
			continue
		}

		if strings.HasSuffix(file.Name(), ".txt") {
			fmt.Printf("file : "+src+file.Name()+"\n");
			ParseFile(src, file.Name(), target);
			continue
		}
		
        
    }

    return nil
}

func ParseFile(src, filename, target string) error {
	file, err := os.Open(src+filename);
	if err != nil {
		fmt.Println("ERROR READ FILE : ");
		fmt.Println(err);
		return err;
	}
	
	defer file.Close();

	timeStart := time.Now();

	scanner := bufio.NewScanner(file);

	pattern_device := regexp.MustCompile("^APPLIANCE\\s:\\s([a-zA-Z0-9\\s\\(\\),]*)");
	pattern_home := regexp.MustCompile("^HOUSEHOLD\\s:\\s([0-9]*)");
	pattern_line := regexp.MustCompile("^(([0-9]{2}\\/?){3})\\t(([0-9]{2}:?){2})\\t([0-9]*)\\t([0-9]*)");

	output_device := "";
	deviceId := strings.Replace(strings.Split(filename,"-")[2], ".txt", "", 1);
	output_home := "";
	homeId := "";
	output_line := "";
	for scanner.Scan() {
		line := scanner.Text();

		results_device := pattern_device.FindStringSubmatch(line);
		results_home := pattern_home.FindStringSubmatch(line);
		results_line := pattern_line.FindStringSubmatch(line);
		if len(results_device) > 0 {
			deviceName := results_device[1];
			output_device += deviceId + ";" + deviceName;
		}
		if len(results_home) > 0 {
			homeId = results_home[1];
			output_home += homeId;
		}
		if len(results_line) > 0 {
			date := results_line[1];
			heure := results_line[3];
			state := results_line[5];
			value := results_line[6];
			output_line += homeId + ";" + deviceId + ";" + strings.Replace(date, "/", "-", 3) + "T" + heure + ";" + state + ";" + value + "\n";
		}
	}

	timeEnd := time.Now();

	fmt.Print("PARSING FILE end (secondes not ms) : ");
	fmt.Print((timeEnd.Sub(timeStart)/1000));
	fmt.Print(" s\n");

	if err := writeCSV(output_device, output_home, output_line, target, filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err
    }

	return nil;
}

func writeCSV(device_data, home_data, line_data, target, filename string) error {
	
	filename = strings.Replace(filename, ".txt", ".csv", 1);

    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIR CSV : ");
		fmt.Println(err);
        return err
    }

	err := ioutil.WriteFile(target + filename, []byte(line_data), 0755);
	if err != nil {
		fmt.Println("ERROR CREATE FILE CSV : ");
		fmt.Println(err);
		return err;
	}

	fmt.Print("LINE WRITE : ");
	fmt.Print(len(line_data));
	fmt.Println(" bytes");

	f, err := os.OpenFile(target + "appliance.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755);
	if err != nil {
		fmt.Println("ERROR CREATE FILE APPLIANCE CSV : ");
		fmt.Println(err);
		return err;
	}

	device_data += "\n";
	nb, err := f.WriteString(device_data);
	if err != nil {
		fmt.Println("ERROR WRITE FILE APPLIANCE CSV : ");
		fmt.Println(err);
		return err;
	}

	fmt.Print("APPLIANCE WRITE : ");
	fmt.Print(nb);
	fmt.Println(" bytes");

	
	fh, err := os.OpenFile(target + "household.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755);
	if err != nil {
		fmt.Println("ERROR CREATE FILE APPLIANCE CSV : ");
		fmt.Println(err);
		return err;
	}

	home_data += "\n";
	nbh, err := fh.WriteString(home_data);
	if err != nil {
		fmt.Println("ERROR WRITE FILE APPLIANCE CSV : ");
		fmt.Println(err);
		return err;
	}

	fmt.Print("HOUSEHOLD WRITE : ");
	fmt.Print(nbh);
	fmt.Println(" bytes");


	fh.Close();

	return nil;
}



func main() {
	fmt.Println("STARTING Parser !");
	if err := os.RemoveAll("output/parsed/"); err != nil {
		fmt.Println("ERROR CLEAR DIST : ");
		fmt.Println(err);
    }
	fmt.Println("CLEARING output : DONE");
	
	timeStart := time.Now();
	Parser("output/unziped/", "output/parsed/");
	timeEnd := time.Now();
	fmt.Print("PARSING end (secondes not ms) : ");
	fmt.Print((timeEnd.Sub(timeStart)/1000));
	fmt.Print(" s\n");
	fmt.Println("PARSING files : DONE");

	fmt.Println("TODO : CLEAN ZIPED FILES");

	fmt.Println("ENDING Parser !");
}
