package main

import (
	"flag"
	"fmt"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/exec"
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

func generateJavaCmdDruidTool(javaClassPath string, dirIn string, fileOut string) string {
	if len(javaClassPath) == 0 || len(dirIn) == 0 || len(fileOut) == 0 {
		return ""
	} else {
		fmt.Printf("javaClassPath: %s\n", javaClassPath)
		return " -classpath " + javaClassPath +
			" io.druid.cli.Main tools dump-segment --directory " +
			dirIn + " --out " + fileOut
	}
}

func (myModel *DataModel) generateCsvFromDruidData(javaClassPath string, csvOut string, tempoFolder string, segPerCsv int) error {
	for  i := 0; i < len(myModel.DruidFiles) && i < 10; i++ {
		tempoFileOut := tempoFolder + "/" + generateTempoFileFromSegment(myModel.DruidFiles[i].DruidFile)
		// javaCmd := generateJavaCmdDruidTool(javaClassPath, myModel.DruidFiles[i].DruidFile, tempoFileOut)
		//path, err := exec.LookPath("java")
		//if err != nil {
		//	global.Logger.WithError(err).Fatal("installing java is in your future")
		//}
		javaClassPathWithQuote := "\"" + javaClassPath + "\""
		cmd := exec.Command("java", "-classpath", javaClassPathWithQuote, "io.druid.cli.Main tools dump-segment --directory", myModel.DruidFiles[i].DruidFile,
			"--out", tempoFileOut)
		fmt.Printf("cmd: %v\n", cmd)
		out, err := cmd.CombinedOutput()
		if err != nil {
			global.Logger.WithError(err).Fatal("cmd.Run() failed")
		}
		fmt.Printf("combined out:\n%s\n", string(out))
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

func generateTempoFileFromSegment(segment string) string {
	result := strings.SplitN(segment, "/", -1)
	amount := len(result)
	return result[amount-3] + "_" + result[amount-2] + "_" + "tempo.json"
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
	for _, model := range myModels {
		err := model.generateCsvFromDruidData(*classPath, *folderCsv, *tempoFolder, segPerCsvInt)
		if err != nil {
			global.Logger.WithFields(logrus.Fields{
				"error": err,
				"model" : model.Model,
			}).Warn("unable to generate csv")
		}
	}
}
