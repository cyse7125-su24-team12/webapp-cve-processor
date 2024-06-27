package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func traverseAndStoreJSONFiles(basePath string) error {
	topic, producer, kafkaErr := SetupKafka()
	if kafkaErr != nil {
		fmt.Printf("SetupKafka failed: %v", kafkaErr)
	}
	defer producer.Close()

	fileCounter := 0
	exclusions := map[string]bool{
		"delta.json":    true,
		"deltaLog.json": true,
	}

	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Check if the file is a JSON file and not in the list of excluded files
		filename := filepath.Base(path)
		if filepath.Ext(filename) == ".json" && !exclusions[filename] {
			fileCounter++
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			SendMessageToKafka(topic, data, producer)
			fmt.Printf("%d\n", fileCounter)
		}
		return nil
	})

	return err
}
