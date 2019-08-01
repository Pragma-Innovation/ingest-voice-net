package main

import (
	"flag"
	"fmt"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
)


type SegmentData struct {
	DruidFile string
	Converted bool
}

type DataModel struct {
	Model string
	DruidFiles []*SegmentData
}

type DataModels []*DataModel

func inspectDruidSegmentCache(segmentLocation string, dataModels *DataModels) error {
	allFilesPath, err := ioutil.ReadDir(segmentLocation)
	if err != nil {
		global.Logger.WithError(err).Fatal("Unable to read druid segment cache folder")
	}
	if len(allFilesPath) == 0 {
		global.Logger.Warn("Druid segment cache folder is empty")
		return fmt.Errorf("cannot deal with emtry druid folder")
	}
	for _, filePath := range allFilesPath {
		if filePath.Name() != ".DS_Store" {
			currentModel := &DataModel {
				Model: filePath.Name(),
			}
			*dataModels = append(*dataModels, currentModel)
		}
	}
	for _, model := range *dataModels {
		modelPath := segmentLocation + "/" + model.Model
		err := filepath.Walk(modelPath, func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) == ".bin" {
				druidFile := &SegmentData{
					DruidFile: path,
					Converted: false,
				}
				model.DruidFiles = append(model.DruidFiles, druidFile)
				return nil
			} else {
				return nil
			}
		})
		if err != nil {
			global.Logger.WithFields(logrus.Fields{
				"error": err,
				"model": model.Model,
			}).Fatal("Unable to read druid segment")
		}
	}
	return nil
}

func main() {
	druidSegmentFolder := flag.String("seg", "", "mandatory - folder where druid segments are stored")
	folderCsv := flag.String("csv", "", "mandatory - folder where druid migration tool store csv files with druid data")
	logLevel := flag.String("log", "", "optional - log level can be: trace debug info warn error fatal panic")
	flag.Usage = func() {
		_, err := fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		if err != nil {
			global.Logger.Fatal("unable to write default usage to stderr")
		}
		flag.PrintDefaults()
	}

	flag.Parse()
	if len(*druidSegmentFolder) == 0 || len(*folderCsv) == 0 {
		flag.Usage()
		global.Logger.Fatal("bad parameters\n")
	}
	if len(*logLevel) != 0 {
		global.SetLogLevel(*logLevel)
		global.Logger.WithField("level", *logLevel).Warn("log level has been modified to this value")
	}
	var myModels DataModels
	err := inspectDruidSegmentCache(*druidSegmentFolder, &myModels)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to inspect druid segment cache folder")
	}
	for _, model := range myModels {
		fmt.Printf("model: %s\n", model.Model)
		for _, segment := range model.DruidFiles {
			fmt.Printf("\tsegment: %s\n", segment.DruidFile)
		}
	}
}