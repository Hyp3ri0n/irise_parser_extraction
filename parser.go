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
	"bytes"
)

type telemetry struct {
    date string
	heure string
	state string
	value string
}

type data struct {
	homeId string
	deviceId string
	deviceName string
    telem []telemetry
}

var DEVICE_TABLE_NAME = "Appliance";
var HOME_TABLE_NAME = "Household";
var TELEMETRY_TABLE_NAME = "Telemetry";

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

	data_to_insert_before := data{deviceId: strings.Replace(strings.Split(filename,"-")[2], ".txt", "", 1)};
	data_to_insert_after := data{deviceId: strings.Replace(strings.Split(filename,"-")[2], ".txt", "", 1)};

	for scanner.Scan() {
		line := scanner.Text();

		results_device := pattern_device.FindStringSubmatch(line);
		results_home := pattern_home.FindStringSubmatch(line);
		results_line := pattern_line.FindStringSubmatch(line);

		if len(results_device) > 0 {
			data_to_insert_before.deviceName = results_device[1];
			data_to_insert_after.deviceName = results_device[1];
		}
		if len(results_home) > 0 {
			data_to_insert_before.homeId = results_home[1];
			data_to_insert_after.homeId = results_home[1];
		}
		if len(results_line) > 0 {
			
			telem := telemetry{
				date: results_line[1],
				heure: results_line[3],
				state: results_line[5],
				value: results_line[6]}
			data_to_insert_before.telem = append(data_to_insert_before.telem, telem);
		}
	}

	timeEnd := time.Now();

	fmt.Print("PARSING FILE end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	for i := 0; i < 10; i++ {
		data_to_insert_after.telem = append(data_to_insert_after.telem, data_to_insert_before.telem[len(data_to_insert_before.telem)-1]);
		data_to_insert_before.telem = data_to_insert_before.telem[:len(data_to_insert_before.telem)-1];
	}

	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
	/*			Architecture A        */
	/*						          */
	/*			Telemetry table :     */
	/*			Architecture A        */
	/*			Architecture A        */
	/*			Architecture A        */
	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
	if err := write_CSV_stratBasic_archA(data_to_insert_before, target+"CSV/arch_a/strat_basic/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
	}
	
	if err := write_CSV_stratOnUpdate_archA(data_to_insert_before, target+"CSV/arch_a/strat_onUpdate/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }
	
	if err := write_SQL_stratBasic_archA(data_to_insert_before, target+"SQL/arch_a/strat_basic/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }
	
	if err := write_SQL_stratOnUpdate_archA(data_to_insert_before, target+"SQL/arch_a/strat_onUpdate/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }
	
	if err := write_SQL_insertAfter_archA(data_to_insert_after, target+"insert_after/arch_a/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }

	return nil;
}

func write_CSV_stratBasic_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".csv", 1);

	// create data
	timeStart := time.Now();

	device_data := data.deviceId + ";" + data.deviceName + "\n";
	home_data := data.homeId + "\n";

	var line_data bytes.Buffer;
	for i := range data.telem {
		line_data.WriteString(data.homeId);
		line_data.WriteString(";");
		line_data.WriteString(data.deviceId);
		line_data.WriteString(";");
		line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
		line_data.WriteString("T");
		line_data.WriteString(data.telem[i].heure);
		line_data.WriteString(";");
		line_data.WriteString(data.telem[i].state);
		line_data.WriteString(";");
		line_data.WriteString(data.telem[i].value);

		if i < (len(data.telem) - 1) {
			line_data.WriteString("\n");
		}
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_CSV_file(line_data.String(), device_data, home_data, filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_CSV_stratOnUpdate_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".csv", 1);

	// create data
	timeStart := time.Now();

	device_data := data.deviceId + ";" + data.deviceName + "\n";
	home_data := data.homeId + "\n";
	previous_value := "";

	var line_data bytes.Buffer;
	for i := range data.telem {
		if previous_value != data.telem[i].value {
			line_data.WriteString(data.homeId);
			line_data.WriteString(";");
			line_data.WriteString(data.deviceId);
			line_data.WriteString(";");
			line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
			line_data.WriteString("T");
			line_data.WriteString(data.telem[i].heure);
			line_data.WriteString(";");
			line_data.WriteString(data.telem[i].state);
			line_data.WriteString(";");
			line_data.WriteString(data.telem[i].value);

			if i < (len(data.telem) - 1) {
				line_data.WriteString("\n");
			}
		}
		previous_value = data.telem[i].value;
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_CSV_file(line_data.String(), device_data, home_data, filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_stratBasic_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".sql", 1);

	// create data
	timeStart := time.Now();

	device_data := "INSERT INTO " + DEVICE_TABLE_NAME + "(id, name) VALUES ('" + data.deviceId + "', '" + data.deviceName + "');\n";
	home_data := "INSERT INTO " + HOME_TABLE_NAME + "(id) VALUES ('" + data.homeId + "');\n";

	var line_data bytes.Buffer;
	for i := range data.telem {
		line_data.WriteString("INSERT INTO ");
		line_data.WriteString(TELEMETRY_TABLE_NAME);
		line_data.WriteString("(datetime, value, state, appliance_id, household_id) VALUES ('");
		line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
		line_data.WriteString("T");
		line_data.WriteString(data.telem[i].heure);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].state);
		line_data.WriteString("', '");
		line_data.WriteString(data.homeId);
		line_data.WriteString("', '");
		line_data.WriteString(data.deviceId);
		line_data.WriteString("');");

		if i < (len(data.telem) - 1) {
			line_data.WriteString("\n");
		}
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_SQL_file(line_data.String(), device_data, home_data, filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_stratOnUpdate_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".sql", 1);

	// create data
	timeStart := time.Now();

	device_data := "INSERT INTO " + DEVICE_TABLE_NAME + "(id, name) VALUES ('" + data.deviceId + "', '" + data.deviceName + "');\n";
	home_data := "INSERT INTO " + HOME_TABLE_NAME + "(id) VALUES ('" + data.homeId + "');\n";

	previous_value := "";

	var line_data bytes.Buffer;
	for i := range data.telem {
		if previous_value != data.telem[i].value {
			line_data.WriteString("INSERT INTO ");
			line_data.WriteString(TELEMETRY_TABLE_NAME);
			line_data.WriteString("(datetime, value, state, appliance_id, household_id) VALUES ('");
			line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
			line_data.WriteString("T");
			line_data.WriteString(data.telem[i].heure);
			line_data.WriteString("', '");
			line_data.WriteString(data.telem[i].value);
			line_data.WriteString("', '");
			line_data.WriteString(data.telem[i].state);
			line_data.WriteString("', '");
			line_data.WriteString(data.homeId);
			line_data.WriteString("', '");
			line_data.WriteString(data.deviceId);
			line_data.WriteString("');");

			if i < (len(data.telem) - 1) {
				line_data.WriteString("\n");
			}
		}
		previous_value = data.telem[i].value;
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_SQL_file(line_data.String(), device_data, home_data, filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_insertAfter_archA(data data, target, filename string) error {

	// We create the folder target
    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIR SQL : ");
		fmt.Println(err);
        return err
    }

	// create data
	timeStart := time.Now();

	var line_data bytes.Buffer;

	line_data.WriteString("-- INSERT DEVICE : " + data.deviceId + " --\n");
	for i := range data.telem {
		line_data.WriteString("INSERT INTO ");
		line_data.WriteString(TELEMETRY_TABLE_NAME);
		line_data.WriteString("(datetime, value, state, appliance_id, household_id) VALUES ('");
		line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
		line_data.WriteString("T");
		line_data.WriteString(data.telem[i].heure);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].state);
		line_data.WriteString("', '");
		line_data.WriteString(data.homeId);
		line_data.WriteString("', '");
		line_data.WriteString(data.deviceId);
		line_data.WriteString("');\n");
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	// We write the CSV fike that contains telemetry's data
	if err := appendFile(target + "insert_after.sql", line_data.String()); err != nil {
		fmt.Println("ERROR WRITE FILE ALL_TELEMETRY SQL : ");
		fmt.Println(err);
		return err;
	}

	return nil;
}

func write_CSV_file(line_data, device_data, home_data, filename, target string) error {
	// We create the folder target
    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIR CSV : ");
		fmt.Println(err);
        return err
    }

	// We write the CSV fike that contains telemetry's data
	if err := appendFile(target + filename, line_data); err != nil {
		fmt.Println("ERROR WRITE FILE CSV : ");
		fmt.Println(err);
		return err;
	}

	// We write the CSV fike that contains telemetry's data
	if err := appendFile(target + "all_telemetry.csv", line_data); err != nil {
		fmt.Println("ERROR WRITE FILE ALL_TELEMETRY CSV : ");
		fmt.Println(err);
		return err;
	}

	// We write the appliance CSV
	if err := appendFile(target + "appliance.csv", device_data); err != nil {
		fmt.Println("ERROR WRITE FILE APPLIANCE CSV : ");
		fmt.Println(err);
		return err;
	}

	// We write the household CSV
	if err := appendFile(target+"household.csv", home_data); err != nil {
		fmt.Println("ERROR WRITE FILE HOUSEHOLD CSV : ");
		fmt.Println(err);
		return err;
	}

	return nil;
}

func write_SQL_file(line_data, device_data, home_data, filename, target string) error {
	// We create the folder target
    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIR SQL : ");
		fmt.Println(err);
        return err
    }

	// We write the CSV fike that contains telemetry's data
	if err := appendFile(target + filename, line_data); err != nil {
		fmt.Println("ERROR WRITE FILE SQL : ");
		fmt.Println(err);
		return err;
	}

	// We write the CSV fike that contains telemetry's data
	if err := appendFile(target + "all_telemetry.sql", line_data); err != nil {
		fmt.Println("ERROR WRITE FILE ALL_TELEMETRY SQL : ");
		fmt.Println(err);
		return err;
	}

	// We write the appliance CSV
	if err := appendFile(target + "appliance.sql", device_data); err != nil {
		fmt.Println("ERROR WRITE FILE APPLIANCE SQL : ");
		fmt.Println(err);
		return err;
	}

	// We write the household CSV
	if err := appendFile(target+"household.sql", home_data); err != nil {
		fmt.Println("ERROR WRITE FILE HOUSEHOLD SQL : ");
		fmt.Println(err);
		return err;
	}

	return nil;
}

func appendFile(target, content string) error {
	f, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755);
	if err != nil {
		return err;
	}

	nbh, err := f.WriteString(content);
	if err != nil {
		return err;
	}

	fmt.Print("APPEND WRITE ON " + target + " : ");
	fmt.Print(nbh);
	fmt.Println(" bytes");


	f.Close();

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
	fmt.Print("PARSING end : ");
	fmt.Println(timeEnd.Sub(timeStart));
	fmt.Println("PARSING files : DONE");

	fmt.Println("TODO : CLEAN ZIPED FILES");

	fmt.Println("ENDING Parser !");
}
