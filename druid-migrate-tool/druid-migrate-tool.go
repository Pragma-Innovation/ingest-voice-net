package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
)

type SegmentData struct {
	DruidFile string
	Converted bool
}

type DataModel struct {
	Model      string
	DruidFiles []*SegmentData
}

type DataModels []*DataModel

type TempoFileDone struct {
	File  string
	Topic string
}

func tempoFileProducer(tempoFile TempoFileDone) error {
	fmt.Printf("<=================>\n")
	fmt.Printf("received this file: %s\n", tempoFile.File)
	fmt.Printf("sending file content to topic: %s\n", tempoFile.Topic)
	fmt.Printf("<=================>\n\n")
	var lineCounter int
	if tempoFileDescr, err := os.Open(tempoFile.File); err == nil {
		defer tempoFileDescr.Close()
		scanner := bufio.NewScanner(tempoFileDescr)
		for scanner.Scan() {
			lineCounter++
		}
	}
	fmt.Printf("produced %d lines\n", lineCounter)
	err := os.Remove(tempoFile.File)
	if err != nil {
		global.Logger.WithError(err).Fatal("Unable to delete temporary druid migration file")
	}
	return nil
}

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
			currentModel := &DataModel{
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

func printStatsOfSegmentsInspection(dataModels DataModels) {
	fmt.Printf("\n\n<========================= Begining of inspection result =============================>\n\n")
	fmt.Printf("Found %d models in druid segment cache\n\n", len(dataModels))
	for _, model := range dataModels {
		fmt.Printf("\n\nmodel: %s have %d segments\n", model.Model, len(model.DruidFiles))
		fmt.Printf("\tTop 10 segments:\n")
		for i := 0; i < len(model.DruidFiles) && i < 10; i++ {
			fmt.Printf("\tsegment: %s\n", model.DruidFiles[i].DruidFile)
		}
		if len(model.DruidFiles) > 10 {
			fmt.Printf("\t...\n\t...\n\t...\n")
			fmt.Printf("\tLast 10 segments:\n")
			for i := len(model.DruidFiles) - 10; i < len(model.DruidFiles); i++ {
				fmt.Printf("\tsegment: %s\n", model.DruidFiles[i].DruidFile)
			}
		}
	}
	fmt.Printf("\n\n<========================= End of inspection result      =============================>\n\n")
}

func (myModel *DataModel) generateCsvFromDruidData(javaClassPath string, csvOut string, tempoFolder string,
	segPerCsv int, producerChan chan TempoFileDone) error {
	for i := 0; i < len(myModel.DruidFiles) && i < 10; i++ {
		tempoFileOut := tempoFolder + "/" + generateTempoFileFromSegment(myModel.DruidFiles[i].DruidFile, myModel.Model)
		cmd := exec.Command("java", "-classpath", javaClassPath, "io.druid.cli.Main", "tools", "dump-segment",
			"--directory", myModel.DruidFiles[i].DruidFile,
			"--out", tempoFileOut)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			global.Logger.WithError(err).Fatal("cmd.Run() failed")
		}
		producerChan <- TempoFileDone{
			File:  tempoFileOut,
			Topic: myModel.Model + "_mig_topic",
		}
	}
	return nil
}

func (myModels DataModels) stripSegFileNameFromPath() {
	for _, model := range myModels {
		for _, segment := range model.DruidFiles {
			segment.DruidFile = strings.Trim(segment.DruidFile, "version.bin")
		}
	}
}

func generateTempoFileFromSegment(segment string, model string) string {
	result := strings.SplitN(segment, "/", -1)
	amount := len(result)
	return model + "_" + result[amount-3] + "_" + result[amount-2] + "_" + "tempo.json"
}

func main() {
	druidSegmentFolder := flag.String("seg", "", "mandatory - folder where druid segments are stored")
	folderCsv := flag.String("csv", "", "mandatory - folder where druid migration tool store csv files with druid data")
	classPath := flag.String("classpath", "", "mandatory - folder where druid java class path are stored")
	tempoFolder := flag.String("tempo", "", "mandatory - folder where storing temporary files (druid segment tool")
	segPerCsv := flag.String("segpercsv", "", "mandatory - amount of segment per csv file")
	logLevel := flag.String("log", "", "optional - log level can be: trace debug info warn error fatal panic")
	flag.Usage = func() {
		_, err := fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		if err != nil {
			global.Logger.Fatal("unable to write default usage to stderr")
		}
		flag.PrintDefaults()
	}

	flag.Parse()
	if len(*druidSegmentFolder) == 0 || len(*folderCsv) == 0 ||
		len(*classPath) == 0 || len(*tempoFolder) == 0 || len(*segPerCsv) == 0 {
		flag.Usage()
		global.Logger.Fatal("bad parameters\n")
	}
	segPerCsvInt, err := strconv.Atoi(*segPerCsv)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to convert the amount of seg par csv")
	}

	if len(*logLevel) != 0 {
		global.SetLogLevel(*logLevel)
		global.Logger.WithField("level", *logLevel).Warn("log level has been modified to this value")
	}
	var myModels DataModels
	err = inspectDruidSegmentCache(*druidSegmentFolder, &myModels)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to inspect druid segment cache folder")
	}
	myModels.stripSegFileNameFromPath()
	printStatsOfSegmentsInspection(myModels)
	migrationToolSignals := make(chan os.Signal, 1)
	signal.Notify(migrationToolSignals, os.Interrupt)
	// channel to close program only once all routines are over
	doneCh := make(chan bool, 1)
	tempoFileCh := make(chan TempoFileDone)
	// go routine to interrupt program
	go func() {
		for mySig := range migrationToolSignals {
			global.Logger.WithField("signal", mySig).Warn("signal received")
			doneCh <- true
		}
	}()
	// routine to send druid dump tool file content to kafka
	go func() {
		for tempoFile := range tempoFileCh {
			if tempoFile.File == "end" {
				global.Logger.Warn("received exiting file end")
				doneCh <- true
				return
			} else {
				err := tempoFileProducer(tempoFile)
				if err != nil {
					global.Logger.WithFields(logrus.Fields{
						"file":  tempoFile.File,
						"topic": tempoFile.Topic,
					}).Warn("unable to produce this file")
				}
			}
		}
	}()
	// go routine to run druid segment dump tool
	go func() {
		for _, model := range myModels {
			err := model.generateCsvFromDruidData(*classPath, *folderCsv, *tempoFolder, segPerCsvInt, tempoFileCh)
			if err != nil {
				global.Logger.WithFields(logrus.Fields{
					"error": err,
					"model": model.Model,
				}).Warn("unable to generate csv")
			}
		}
		tempoFileCh <- TempoFileDone {
			File:  "end",
			Topic: "end",
		}
		global.Logger.Info("exiting loop running druid migration tool")
		return
	}()
	global.Logger.Info("Migration tool launched ... waiting for signals")
	<-doneCh
	global.Logger.Info("Interrupt received exiting migration tool ...")
}
