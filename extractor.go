package main

import (
	"fmt"
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func Unzip(archive, target string) error {
    reader, err := zip.OpenReader(archive)
    if err != nil {
		fmt.Println("ERROR OPEN ARCHIVE : ");
		fmt.Println(err);
        return err
    }

    if err := os.MkdirAll(target, 0755); err != nil {
		fmt.Println("ERROR CREATE DIST : ");
		fmt.Println(err);
        return err
    }

    for _, file := range reader.File {

		fmt.Printf("file : "+file.Name+"\n");

		path := filepath.Join(target, file.Name)
		
		if strings.HasSuffix(file.Name, ".zip") {
			copyFile(file, path);
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

func main() {
	fmt.Println("STARTING Extractor !");
	if err := os.RemoveAll("output/unziped/"); err != nil {
		fmt.Println("ERROR CLEAR DIST : ");
		fmt.Println(err);
    }
	fmt.Println("CLEARING output : DONE");

	Unzip("dataTEST.zip", "output/unziped/");
	fmt.Println("UNZIPPING files : DONE");

	fmt.Println("TODO : CLEAN ZIP");

	fmt.Println("ENDING Extractor !");
}
