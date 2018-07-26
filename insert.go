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

type MyDB struct {
    *sql.DB
}

var insertData = [182]InsertData{};
var durations = []time.Duration{};

func Insert(src string) error {
	// Chaine de connexion à la base de données PostgreSQL
	connStr := "postgres://postgres:root@localhost/irise_opti?sslmode=disable"

	// Connexion
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err;
	}
	fmt.Println("CONNECTED TO DB !");
	
	// Lecture du fichier d'insert
	file, err := os.Open(src);
	if err != nil {
		fmt.Println("ERROR READ FILE : ");
		fmt.Println(err);
		return err;
	}

	fmt.Println("OPEN FILE DONE !");

	// Permet de deferer la fermeture du fichier dès qu'il n'est plus utilisé
	defer file.Close();

	pattern_split := regexp.MustCompile("^--*");
	index := -1;
	start := false;
	scanner := bufio.NewScanner(file);

	// Lecture des lignes du fichier
	for scanner.Scan() {
		line := scanner.Text();

		// Recupère les lignes de commentaire (^--*)
		split := pattern_split.FindStringSubmatch(line);

		// Si il s'agit d'une ligne de commentaire
		if len(split) > 0 {
			// Voir la strcuture des fichier générer
			// il y a deux lignes de commentaire, une sans ID, une avec ID
			start = !start;
			// si c'est une ligne de commentaire avec ID => changement d'appareil
			if start == true {
				index = index+1;
			}
		} else {
			// Si ce n'est pas une ligne vide, on insert la commande dans un tableau
			if len(line) > 0 {
				insertData[index].data = append(insertData[index].data, line);
			}
		}

	}
	fmt.Println("PARSE FILE DONE !");

	// pour les 10 lignes
	for j := 0; j < 10; j++ {
		// des 182 appareils
		for i := 0; i < 182; i++ {
			// Code d'insertion des données
			// le keyword 'go' permet de paralleliser l'exécution
			go InsertConcurrence(db, insertData[i].data[j]);
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

func InsertConcurrence(db *sql.DB, request string) {
	timeStart := time.Now();
	// Exécution de la requête
	_, err := db.Exec(request);
	if err != nil {
		fmt.Print("x");
	} else {
		fmt.Print(".");
	}
	timeEnd := time.Now();
	durations = append(durations, timeEnd.Sub(timeStart));
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
