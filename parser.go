package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"bufio"
	"regexp"
	"time"
	"bytes"
)

/**
 * Structure représentant une mesure d'un capteur
 */
type telemetry struct {
    date string
	heure string
	state string
	value string
}

/**
 * Structure représentant une donnée "telemetry" à insérer en base
 */
type data struct {
	homeId string
	deviceId string
	deviceName string
	typeId string
    telem []telemetry
}

/**
 * Structure représentant une donnée "ApplianceType" à insérer en base
 */
type typeDevice struct {
	typeId string
	name string
	lexique []string	// Cette variable permet de savoir les mots clés définissant le type de capteur
}

/**
 * Nom des tables SQL
 */
var DEVICE_TABLE_NAME = "Appliance";
var HOME_TABLE_NAME = "Household";
var TELEMETRY_TABLE_NAME = "Telemetry";
var TYPE_TABLE_NAME = "ApplianceType";

/**
 * Liste des types de capteurs à identifier
 */
var typeData = []typeDevice { 
    typeDevice { typeId: "0", name: "Fridge - Freezer", lexique: []string{"fridge", "freezer"} },
    typeDevice { typeId: "1", name: "Lamp", lexique: []string{"lamp", "light", "Halogen"} },
    typeDevice { typeId: "2", name: "Microwave", lexique: []string{"Microwave"} },
    typeDevice { typeId: "3", name: "Washing machine", lexique: []string{"Washing"} },
    typeDevice { typeId: "4", name: "Electric Cooker - heating", lexique: []string{"Cooker", "heating", "plate"} },
    typeDevice { typeId: "5", name: "Power supply wood - fuel", lexique: []string{"Power supply"} },
    typeDevice { typeId: "6", name: "Dish washer", lexique: []string{"Dish"} },
    typeDevice { typeId: "7", name: "Heat pump", lexique: []string{"pump"} },
    typeDevice { typeId: "8", name: "TV", lexique: []string{"TV"} },
    typeDevice { typeId: "9", name: "Autres", lexique: []string{} },
};

/**
 * Buffer des données à inserer dans les fichiers
 */
var home_data_csv bytes.Buffer;
var home_data_sql bytes.Buffer;
var type_data_csv bytes.Buffer;
var type_data_sql bytes.Buffer;


/**
 * Le point d'entré du programme :
 * Ce dernier permet de parser les fichiers "*.txt" issue de l'extraction de l'archive IRISE
 * /!\ Ce script génère les données pour deux strategies et pour deux architectures en "*.csv" et "*.sql" /!\
 * Prerequis :
 *	- le script "extractor.go" doit être lancé et terminé
 *	- Le dossier "output/unziped" doit être au même niveau que les scripts go
 * Architecture produite :
 *  output/
 *  |___ parsed/
 *    	|___ CSV/
 *    	|	|___ arch_a/
 *    	|	|	|___ start_basic/
 *    	|	|	|	|___ *.csv
 *    	|	|	|___ strat_onUpdate/
 *    	|	|	 	|___ *.csv
 *    	|	|___ arch_b/
 *    	|		|___ start_basic/
 *    	|		|	|___ *.csv
 *    	|		|___ strat_onUpdate/
 *    	|			|___ *.csv
 *    	|___ insert_after/
 *    	|	|___ arch_a/
 *    	|	|	|___ start_basic/
 *    	|	|	|	|___ insert_after.sql
 *    	|	|	|___ strat_onUpdate/
 *    	|	|		|___ insert_after.sql
 *    	|	|___ arch_b/
 *    	|		|___ start_basic/
 *    	|		|	|___ insert_after.sql
 *    	|		|___ strat_onUpdate/
 *    	|			|___ insert_after.sql
 *    	|___ SQL/
 *    		|___ arch_a/
 *    		|	|___ start_basic/
 *    		|	|	|___ *.sql
 *    		|	|___ strat_onUpdate/
 *    		|		|___ *.sql
 *    		|___ arch_b/
 *    			|___ start_basic/
 *    			|	|___ *.sql
 *    			|___ strat_onUpdate/
 *    				|___ *.sql
 */
func main() {
	fmt.Println("STARTING Parser !");
	// Supprime le dossiers "output/parsed" si il est déjà présent
	if err := os.RemoveAll("output/parsed/"); err != nil {
		fmt.Println("ERROR CLEAR DIST : ");
		fmt.Println(err);
    }
	fmt.Println("CLEARING output : DONE");
	
	timeStart := time.Now();
	
	// Execution de la fonction principale
	Parser("output/unziped/", "output/parsed/");

	timeEnd := time.Now();
	fmt.Print("PARSING end : ");
	fmt.Println(timeEnd.Sub(timeStart));
	fmt.Println("PARSING files : DONE");

	fmt.Println("TODO : CLEAN ZIPED FILES");

	fmt.Println("ENDING Parser !");
}

/**
 * Cette méthode permet de parser les fichiers "*.txt" présent dans les sources 
 * @src le chemin sources des fichiers
 * @target le chemin cible de génération
 */
func Parser(src, target string) error {
	// On récupère les fichiers "*.txt"
	files, err := ioutil.ReadDir(src)
    if err != nil {
		fmt.Println("ERROR READ DIR : ");
		fmt.Println(err);
		return err
    }

	// On créé le dossier cible
    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err
    }

	// Pour chaque fichier
    for _, file := range files {

		// Si c'est un dossier
        if file.IsDir() {
			// Pn relance le parsage à l'intérieure
			Parser(src + file.Name() +"/", target);
            continue
		}
		
		// Si c'est un zip on ne fait rien
		if strings.HasSuffix(file.Name(), ".zip") {
			continue
		}

		// Si c'est un fichier "*.txt"
		if strings.HasSuffix(file.Name(), ".txt") {
			fmt.Printf("file : "+src+file.Name()+"\n");
			// On parse le fichier
			ParseFile(src, file.Name(), target);
			continue
		}
		
        
	}
	
    return nil
}

/**
 * Cette méthode permet de parser un fichier "*.txt" 
 * @src le chemin source du fichier
 * @filename le nom du fichier
 * @target le chemin cible de génération
 */
func ParseFile(src, filename, target string) error {
	// On ouvre le fichier
	file, err := os.Open(src+filename);
	if err != nil {
		fmt.Println("ERROR READ FILE : ");
		fmt.Println(err);
		return err;
	}
	
	defer file.Close();

	timeStart := time.Now();

	// On ouvre un scanner pour lire le fichier
	scanner := bufio.NewScanner(file);

	// On définit les regex pour parser les données
	pattern_device := regexp.MustCompile("^APPLIANCE\\s:\\s([a-zA-Z0-9\\s\\(\\),]*)");
	pattern_home := regexp.MustCompile("^HOUSEHOLD\\s:\\s([0-9]*)");
	pattern_line := regexp.MustCompile("^(([0-9]{2}\\/?){3})\\t(([0-9]{2}:?){2})\\t([0-9]*)\\t([0-9]*)");

	data_to_insert_before := data{deviceId: strings.Replace(strings.Split(filename,"-")[2], ".txt", "", 1)};
	data_to_insert_after := data{deviceId: strings.Replace(strings.Split(filename,"-")[2], ".txt", "", 1)};

	// On lit le fichier
	for scanner.Scan() {
		line := scanner.Text();

		// On applique les regex
		results_device := pattern_device.FindStringSubmatch(line);
		results_home := pattern_home.FindStringSubmatch(line);
		results_line := pattern_line.FindStringSubmatch(line);

		// On insert les données recoltés pour le capteur
		if len(results_device) > 0 {
			data_to_insert_before.deviceName = results_device[1];
			data_to_insert_after.deviceName = results_device[1];

			data_to_insert_before.typeId = "9";
			data_to_insert_after.typeId = "9";

			for _, typeDevice := range typeData {
				for _, word := range typeDevice.lexique {
					if (strings.Contains(data_to_insert_before.deviceName, word)) {
						data_to_insert_before.typeId = typeDevice.typeId;
						data_to_insert_after.typeId = typeDevice.typeId;
						break;
					}
				}
				if (data_to_insert_before.typeId != "9") {
					break;
				}
			}

			
		}
		// On insert les données recoltés pour la maison
		if len(results_home) > 0 {
			data_to_insert_before.homeId = results_home[1];
			data_to_insert_after.homeId = results_home[1];
		}
		// On insert les données recoltés pour les mesures
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

	// On récupère les dernières mesures pour générer le fichier d'insert "au file de l'eau"
	for i := 0; i < 10; i++ {
		data_to_insert_after.telem = append(data_to_insert_after.telem, data_to_insert_before.telem[len(data_to_insert_before.telem)-1]);
		data_to_insert_before.telem = data_to_insert_before.telem[:len(data_to_insert_before.telem)-1];
	}

	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
	/*	   Ecriture Architecture A    */
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
	
	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
	/*	   Ecriture Architecture B    */
	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
	if err := write_CSV_stratBasic_archB(data_to_insert_before, target+"CSV/arch_b/strat_basic/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
	}
	
	if err := write_CSV_stratOnUpdate_archB(data_to_insert_before, target+"CSV/arch_b/strat_onUpdate/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }
	
	if err := write_SQL_stratBasic_archB(data_to_insert_before, target+"SQL/arch_b/strat_basic/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }
	
	if err := write_SQL_stratOnUpdate_archB(data_to_insert_before, target+"SQL/arch_b/strat_onUpdate/", filename); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err;
    }
	
	if err := write_SQL_insertAfter_archB(data_to_insert_after, target+"insert_after/arch_b/", filename); err != nil {
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

	device_data := data.deviceId + ";" + data.deviceName + ";" + data.typeId + "\n";
	home_temp_data := data.homeId + "\n";

	var home_data bytes.Buffer;
	if (strings.Contains(home_data_csv.String(), home_temp_data) == false) {
		home_data_csv.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

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
		line_data.WriteString("\n");
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_CSV_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_CSV_stratOnUpdate_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".csv", 1);

	// create data
	timeStart := time.Now();

	device_data := data.deviceId + ";" + data.deviceName +  ";" + data.typeId + "\n";
	home_temp_data := data.homeId + "\n";

	var home_data bytes.Buffer;
	if (strings.Contains(home_data_csv.String(), home_temp_data) == false) {
		home_data_csv.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

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
			line_data.WriteString("\n");
		}
		previous_value = data.telem[i].value;
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_CSV_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_stratBasic_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".sql", 1);

	// create data
	timeStart := time.Now();

	device_data := "INSERT INTO " + DEVICE_TABLE_NAME + "(id, name, appliancetype_id) VALUES ('" + data.deviceId + "', '" + data.deviceName + "', '" + data.typeId + "');\n";
	home_temp_data := "INSERT INTO " + HOME_TABLE_NAME + "(id) VALUES ('" + data.homeId + "');\n";

	var home_data bytes.Buffer;
	if (strings.Contains(home_data_sql.String(), home_temp_data) == false) {
		home_data_sql.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

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
		line_data.WriteString("');\n");
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_SQL_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_stratOnUpdate_archA(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".sql", 1);

	// create data
	timeStart := time.Now();

	device_data := "INSERT INTO " + DEVICE_TABLE_NAME + "(id, name, appliancetype_id) VALUES ('" + data.deviceId + "', '" + data.deviceName + "', '" + data.typeId + "');\n";
	home_temp_data := "INSERT INTO " + HOME_TABLE_NAME + "(id) VALUES ('" + data.homeId + "');\n";
	
	var home_data bytes.Buffer;
	if (strings.Contains(home_data_sql.String(), home_temp_data) == false) {
		home_data_sql.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

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
			line_data.WriteString("');\n");
		}
		previous_value = data.telem[i].value;
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_SQL_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
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

	line_data.WriteString("--" + data.deviceId + "\n");
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
		line_data.WriteString(data.deviceId);
		line_data.WriteString("', '");
		line_data.WriteString(data.homeId);
		line_data.WriteString("');--");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("\n");
	}
	line_data.WriteString("----------------------------------------------\n");

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

func write_CSV_stratBasic_archB(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".csv", 1);

	// create data
	timeStart := time.Now();

	device_data := data.deviceId + ";" + data.deviceName + ";" + data.typeId + ";" + data.homeId + "\n";
	home_temp_data := data.homeId + "\n";

	var home_data bytes.Buffer;
	if (strings.Contains(home_data_csv.String(), home_temp_data) == false) {
		home_data_csv.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

	var line_data bytes.Buffer;
	for i := range data.telem {
		line_data.WriteString(data.deviceId);
		line_data.WriteString(";");
		line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
		line_data.WriteString("T");
		line_data.WriteString(data.telem[i].heure);
		line_data.WriteString(";");
		line_data.WriteString(data.telem[i].state);
		line_data.WriteString(";");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("\n");
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_CSV_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_CSV_stratOnUpdate_archB(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".csv", 1);

	// create data
	timeStart := time.Now();

	device_data := data.deviceId + ";" + data.deviceName +  ";" + data.typeId + ";" + data.homeId + "\n";
	home_temp_data := data.homeId + "\n";

	var home_data bytes.Buffer;
	if (strings.Contains(home_data_csv.String(), home_temp_data) == false) {
		home_data_csv.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

	previous_value := "";
	var line_data bytes.Buffer;
	for i := range data.telem {
		if previous_value != data.telem[i].value {
			line_data.WriteString(data.deviceId);
			line_data.WriteString(";");
			line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
			line_data.WriteString("T");
			line_data.WriteString(data.telem[i].heure);
			line_data.WriteString(";");
			line_data.WriteString(data.telem[i].state);
			line_data.WriteString(";");
			line_data.WriteString(data.telem[i].value);
			line_data.WriteString("\n");
		}
		previous_value = data.telem[i].value;
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_CSV_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_stratBasic_archB(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".sql", 1);

	// create data
	timeStart := time.Now();

	device_data := "INSERT INTO " + DEVICE_TABLE_NAME + "(id, name, appliancetype_id, household_id) VALUES ('" + data.deviceId + "', '" + data.deviceName + "', '" + data.typeId + "', '" + data.homeId + "');\n";
	home_temp_data := "INSERT INTO " + HOME_TABLE_NAME + "(id) VALUES ('" + data.homeId + "');\n";

	var home_data bytes.Buffer;
	if (strings.Contains(home_data_sql.String(), home_temp_data) == false) {
		home_data_sql.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

	var line_data bytes.Buffer;
	for i := range data.telem {
		line_data.WriteString("INSERT INTO ");
		line_data.WriteString(TELEMETRY_TABLE_NAME);
		line_data.WriteString("(datetime, value, state, appliance_id) VALUES ('");
		line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
		line_data.WriteString("T");
		line_data.WriteString(data.telem[i].heure);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].state);
		line_data.WriteString("', '");
		line_data.WriteString(data.deviceId);
		line_data.WriteString("');\n");
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_SQL_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_stratOnUpdate_archB(data data, target, filename string) error {
	
	// We change the extension of the target (txt to CSV)
	filename = strings.Replace(filename, ".txt", ".sql", 1);

	// create data
	timeStart := time.Now();

	device_data := "INSERT INTO " + DEVICE_TABLE_NAME + "(id, name, appliancetype_id, household_id) VALUES ('" + data.deviceId + "', '" + data.deviceName + "', '" + data.typeId + "', '" + data.homeId + "');\n";
	home_temp_data := "INSERT INTO " + HOME_TABLE_NAME + "(id) VALUES ('" + data.homeId + "');\n";
	
	var home_data bytes.Buffer;
	if (strings.Contains(home_data_sql.String(), home_temp_data) == false) {
		home_data_sql.WriteString(home_temp_data);
		home_data.WriteString(home_temp_data);
	}

	previous_value := "";

	var line_data bytes.Buffer;
	for i := range data.telem {
		if previous_value != data.telem[i].value {
			line_data.WriteString("INSERT INTO ");
			line_data.WriteString(TELEMETRY_TABLE_NAME);
			line_data.WriteString("(datetime, value, state, appliance_id) VALUES ('");
			line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
			line_data.WriteString("T");
			line_data.WriteString(data.telem[i].heure);
			line_data.WriteString("', '");
			line_data.WriteString(data.telem[i].value);
			line_data.WriteString("', '");
			line_data.WriteString(data.telem[i].state);
			line_data.WriteString("', '");
			line_data.WriteString(data.deviceId);
			line_data.WriteString("');\n");
		}
		previous_value = data.telem[i].value;
	}

	timeEnd := time.Now();

	fmt.Print("CREATE BUFFER end : ");
	fmt.Println(timeEnd.Sub(timeStart));

	if err := write_SQL_file(line_data.String(), device_data, home_data.String(), filename, target); err != nil {
		return err;
	}

	return nil;
}

func write_SQL_insertAfter_archB(data data, target, filename string) error {

	// We create the folder target
    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIR SQL : ");
		fmt.Println(err);
        return err
    }

	// create data
	timeStart := time.Now();

	var line_data bytes.Buffer;

	line_data.WriteString("--" + data.deviceId + "\n");
	for i := range data.telem {
		line_data.WriteString("INSERT INTO ");
		line_data.WriteString(TELEMETRY_TABLE_NAME);
		line_data.WriteString("(datetime, value, state, appliance_id) VALUES ('");
		line_data.WriteString(strings.Replace(data.telem[i].date, "/", "-", 3));
		line_data.WriteString("T");
		line_data.WriteString(data.telem[i].heure);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("', '");
		line_data.WriteString(data.telem[i].state);
		line_data.WriteString("', '");
		line_data.WriteString(data.deviceId);
		line_data.WriteString("');--");
		line_data.WriteString(data.telem[i].value);
		line_data.WriteString("\n");
	}
	line_data.WriteString("----------------------------------------------\n");
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

	if (type_data_csv.String() == "") {
		for _, typeDevice := range typeData {
			type_data_csv.WriteString(typeDevice.typeId);
			type_data_csv.WriteString(";");
			type_data_csv.WriteString(typeDevice.name);
			type_data_csv.WriteString("\n");
		}

		// We write the appliance_type CSV
		if err := appendFile(target+"appliance_type.csv", type_data_csv.String()); err != nil {
			fmt.Println("ERROR WRITE FILE APPLIANCE TYPE CSV : ");
			fmt.Println(err);
			return err;
		}
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

	if (type_data_sql.String() == "") {
		for _, typeDevice := range typeData {
			type_data_sql.WriteString(typeDevice.typeId);
			type_data_sql.WriteString(";");
			type_data_sql.WriteString(typeDevice.name);
			type_data_sql.WriteString("\n");
		}

		// We write the appliance_type CSV
		if err := appendFile(target+"appliance_type.sql", type_data_sql.String()); err != nil {
			fmt.Println("ERROR WRITE FILE APPLIANCE TYPE SQL : ");
			fmt.Println(err);
			return err;
		}
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