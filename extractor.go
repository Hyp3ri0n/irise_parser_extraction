package main

import (
	"fmt"
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

/**
 * Le point d'entré du programme :
 * Ce dernier permet d'extraire l'archive IRISE afin de pouvoir par la suite
 * parser les fichiers "*.txt"
 * Prerequis :
 *	- l'archive doit être nommée "data.zip"
 *	- l'archive doit être positionnée à "côté" des scripts go (ou des executables si ils sont générés)
 * Architecture produite :
 *    - output/
 *      | unziped/
 *    	|___ data/
 *    		|___ 20000900-Fridge/
 *    			|___ Enertech/
 *    			|	|___ Campagnes/
 *    			|		|___ Remodece/
 *    			|			|___ Travail/
 *    			|				|___ Files/
 *    			|					|___ 1000080-2000900-3009900.txt
 *    			| .../
 *    			| .../
 *    			| .../
 */
func main() {
	fmt.Println("STARTING Extractor !");
	// Supprime le dossiers "output/unzipped" si il est déjà présent
	if err := os.RemoveAll("output/unziped/"); err != nil {
		fmt.Println("ERROR CLEAR DIST : ");
		fmt.Println(err);
    }
	fmt.Println("CLEARING output : DONE");

	// Execution de la fonction principale
	Unzip("data.zip", "output/unziped/");
	fmt.Println("UNZIPPING files : DONE");

	fmt.Println("TODO : CLEAN ZIP");

	fmt.Println("ENDING Extractor !");
}

/**
 * Cette méthode permet de dézipper une archive ainsi que les archives à l'intérieure
 * @archive le chemin de l'archive
 * @target le chemin cible d'extraction
 */
func Unzip(archive, target string) error {
	// Ouverture de l'archive
    reader, err := zip.OpenReader(archive)
    if err != nil {
		fmt.Println("ERROR OPEN ARCHIVE : ");
		fmt.Println(err);
        return err
    }

	// Création du dossier de cible
    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err
    }

	// Pour chaque fichier
    for _, file := range reader.File {

		fmt.Printf("file : "+file.Name+"\n");

		path := filepath.Join(target, file.Name)
		
		// Si le fichier est un fichier zip
		if strings.HasSuffix(file.Name, ".zip") {
			copyFile(file, path);
			// On le dezipe
			Unzip(path, strings.Replace(path, ".zip", "", 1));
			continue
		}

        if file.FileInfo().IsDir() {
            os.MkdirAll(path, file.Mode())
            continue
        }

		if err := copyFile(file, path); err != nil {
			fmt.Println("ERROR COPY : ");
			fmt.Println(err);
			return err
		}
		
    }

    return nil
}

/**
 * Cette méthode permet de copier un fichier
 * @file le fichier à copier
 * @target le chemin cible de copie
 */
func copyFile(file *zip.File, target string) error {

	target_dir := strings.Split(target, "\\");

    if err := os.MkdirAll(strings.Replace(target, "\\"+target_dir[len(target_dir)-1], "", 1), 0755); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err
	}
	
	fileReader, err := file.Open();
	if err != nil {

		if fileReader != nil {
			fileReader.Close()
		}
		fmt.Println("ERROR OPEN SRC : ");
		fmt.Println(err);
		return err
	}

	targetFile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		fileReader.Close()

		if targetFile != nil {
			targetFile.Close()
		}

		fmt.Println("ERROR OPEN DIST : ");
		fmt.Println(err);
		return err
	}

	if _, err := io.Copy(targetFile, fileReader); err != nil {
		fileReader.Close()
		targetFile.Close()

		fmt.Println("ERROR COPY : ");
		fmt.Println(err);
		return err
	}

	fileReader.Close()
	targetFile.Close()

	return nil;
}
