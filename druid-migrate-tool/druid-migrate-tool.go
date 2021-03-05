/*
Ingest Voice Net
Copyright (C) 2021 Pragma Innovation

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
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

type UserKafkaConfig struct {
	BrokerListWithPort []string `json:"kafka-brokers,omitempty"`
	Topic              string   `json:"kafka-topic,omitempty"`
	MessageKey         string   `json:"kafka-msg-key,omitempty"`
	KafkaCertFile      string   `json:"kafka-tls-cert-file,omitempty"`
	KafkaKeyFile       string   `json:"kafka-tls-Key-file,omitempty"`
	KafkaCaFile        string   `json:"kafka-tls-Ca-file,omitempty"`
	KafkaVerifySsl     bool     `json:"kafka-tls-vfy-bool,omitempty"`
}

var KafkaMigProducerConf UserKafkaConfig

func ReadKafkaProducerConfigFromFile(myFile string, myKafkaConf *UserKafkaConfig) error {
	dataBuf, err := ioutil.ReadFile(myFile)
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"file":  myFile,
			"error": err,
		}).Fatal("Unable to read global kafka parameter JSON file")
	}
	err = json.Unmarshal(dataBuf, &myKafkaConf)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to convert Kafka JSON into a struct")
		return err
	}
	global.Logger.Info("Global Kafka parameters loaded")
	return nil
}

func createTlsConfiguration(myKafkaCertFile string, myKafkaKeyFile string,
	myKafkaCaFile string, myKafkaVerifySsl bool) (t *tls.Config) {
	if myKafkaCertFile != "" && myKafkaKeyFile != "" && myKafkaCaFile != "" {
		cert, err := tls.LoadX509KeyPair(myKafkaCertFile, myKafkaKeyFile)
		if err != nil {
			global.Logger.WithError(err).Fatal("Unable to load X509 keys")
		}

		caCert, err := ioutil.ReadFile(myKafkaCaFile)
		if err != nil {
			global.Logger.WithError(err).Fatal("Unable to read Cert file")
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: myKafkaVerifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}

func NewKafkaAsynProducer(myKafkaConfig UserKafkaConfig) sarama.AsyncProducer {
	// let's check if we have all we need
	if len(myKafkaConfig.BrokerListWithPort) == 0 {
		flag.PrintDefaults()
		global.Logger.Fatal("missing required argument for Kafka producer")
	}
	// create snmp stats producer on kafka broker
	// no need to check return as program exits if kafka fails
	producerConfig := sarama.NewConfig()
	producerTlsConfig := createTlsConfiguration(myKafkaConfig.KafkaCertFile,
		myKafkaConfig.KafkaKeyFile, myKafkaConfig.KafkaCaFile,
		myKafkaConfig.KafkaVerifySsl)
	if producerTlsConfig != nil {
		producerConfig.Net.TLS.Enable = true
		producerConfig.Net.TLS.Config = producerTlsConfig
	}
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll         // Only wait for the leader to ack
	producerConfig.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	producerConfig.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	// assumes we fill up the queue un in this timeout
	producerConfig.ChannelBufferSize = 4000 // Buffer of messages
	cdrProducer, err := sarama.NewAsyncProducer(myKafkaConfig.BrokerListWithPort, producerConfig)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to launch kafka producer")
	}
	return cdrProducer
}

func tempoFileProducer(tempoFile TempoFileDone, migProducer sarama.AsyncProducer) error {
	fmt.Printf("<=================>\n")
	fmt.Printf("received this file: %s\n", tempoFile.File)
	fmt.Printf("sending file content to topic: %s\n", tempoFile.Topic)
	fmt.Printf("<=================>\n\n")
	var lineCounter int
	var kafkaErrors int
	betterJson := strings.NewReplacer("\":", "\": ", ",\"", ", \"", "null", "\"\"")
	if tempoFileDescr, err := os.Open(tempoFile.File); err == nil {
		defer tempoFileDescr.Close()
		scanner := bufio.NewScanner(tempoFileDescr)
		for scanner.Scan() {
			cleanerJson := betterJson.Replace(scanner.Text())
			cdrMessage := cleanerJson + "\n"
			msg := &sarama.ProducerMessage{
				Topic: tempoFile.Topic,
				Key:   sarama.StringEncoder("no key"),
				Value: sarama.StringEncoder(cdrMessage)}
			select {
			case migProducer.Input() <- msg:
				lineCounter++
			case err := <-migProducer.Errors():
				kafkaErrors++
				global.Logger.WithError(err).Warn("Failed to produce message")
			}
		}
	}
	fmt.Printf("produced %d lines\n", lineCounter)
	err := os.Remove(tempoFile.File)
	if err != nil {
		global.Logger.WithError(err).Fatal("Unable to delete temporary druid migration file")
	}
	return nil
}

func inspectDruidSegmentCache(segmentLocation string, modelOnly string, fileFilter string,
	dataModels *DataModels) error {
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
			if len(modelOnly) != 0 && filePath.Name() == modelOnly {
				currentModel := &DataModel{
					Model: filePath.Name(),
				}
				*dataModels = append(*dataModels, currentModel)
			} else if len(modelOnly) == 0 {
				currentModel := &DataModel{
					Model: filePath.Name(),
				}
				*dataModels = append(*dataModels, currentModel)
			}
		}
	}
	for _, model := range *dataModels {
		modelPath := segmentLocation + "/" + model.Model
		err := filepath.Walk(modelPath, func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) == ".bin" && strings.Contains(path, fileFilter) {
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

func (myModel *DataModel) generateCsvFromDruidData(javaClassPath string, tempoFolder string,
	producerChan chan TempoFileDone) error {
	for i := 0; i < len(myModel.DruidFiles); i++ {
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
	classPath := flag.String("classpath", "", "mandatory - folder where druid java class path are stored")
	tempoFolder := flag.String("tempo", "", "mandatory - folder where storing temporary files (druid segment tool")
	logLevel := flag.String("log", "", "optional - log level can be: trace debug info warn error fatal panic")
	modelOnly := flag.String("model", "", "optional - give the name a a specific model you want to migrate")
	fileFilter := flag.String("filter", "", "optional - substring that segment name needs to include to be processed")
	kafkaConfFile := flag.String("kafka", "", "mandatory - path to the kafka producer configuration")
	flag.Usage = func() {
		_, err := fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		if err != nil {
			global.Logger.Fatal("unable to write default usage to stderr")
		}
		flag.PrintDefaults()
	}

	flag.Parse()
	if len(*druidSegmentFolder) == 0 || len(*kafkaConfFile) == 0 ||
		len(*classPath) == 0 || len(*tempoFolder) == 0 {
		flag.Usage()
		global.Logger.Fatal("bad parameters\n")
	}
	if len(*logLevel) != 0 {
		global.SetLogLevel(*logLevel)
		global.Logger.WithField("level", *logLevel).Warn("log level has been modified to this value")
	}
	err := ReadKafkaProducerConfigFromFile(*kafkaConfFile, &KafkaMigProducerConf)
	if err != nil {
		global.Logger.Warn("unable to load kafka global configuration file, keeping default")
	}
	// we create an async producer, if it fails program will end
	migProducer := NewKafkaAsynProducer(KafkaMigProducerConf)
	if err != nil {
		global.Logger.Fatal("unable to launch kafka async producer")
	}
	var myModels DataModels
	err = inspectDruidSegmentCache(*druidSegmentFolder, *modelOnly, *fileFilter, &myModels)
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
				err := tempoFileProducer(tempoFile, migProducer)
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
			err := model.generateCsvFromDruidData(*classPath, *tempoFolder, tempoFileCh)
			if err != nil {
				global.Logger.WithFields(logrus.Fields{
					"error": err,
					"model": model.Model,
				}).Warn("unable to generate csv")
			}
		}
		tempoFileCh <- TempoFileDone{
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
