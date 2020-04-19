package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"github.com/Pragma-Innovation/ingest-voice-net/cdr-tools"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/Shopify/sarama"
	"github.com/flosch/pongo2"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

// all types an related method for argument flags

type fileList []string

func (myFileList *fileList) String() string {
	return fmt.Sprintf("%s", *myFileList)
}

func (myFileList *fileList) Set(value string) error {
	*myFileList = append(*myFileList, value)
	return nil
}

type kafkaBrokerList []string

func (myBrokerList *kafkaBrokerList) String() string {
	return fmt.Sprintf("%s", *myBrokerList)
}

func (myBrokerList *kafkaBrokerList) Set(value string) error {
	*myBrokerList = append(*myBrokerList, value)
	return nil
}

var userCdrFiles fileList
var userKafkaBrokerList kafkaBrokerList

var (
	userMode               = flag.String("mode", "", "MANDATORY: mode of execution, could be \"batch\" or \"micro-batch\"")
	userDruidBatchFqdn     = flag.String("batch-fqdn", "", "MANDATORY BATCH MODE: gives the fqdn of druid cluster for batch ingest")
	userDruidIntervals     = flag.String("batch-intervals", "", "MANDATORY BATCH MODE: gives intervals of time of batch ingestion")
	userDruidSchemaTpl     = flag.String("batch-tpl", "", "MANDATORY BATCH MODE: Template file druid schema for batch ingest")
	userKafkaBrokerPort    = flag.String("kafka-port", "", "MANDATORY STREAM MODE: TCP port of Kafka broker")
	userKafkaDruidCdrTopic = flag.String("kafka-topic", "", "MANDATORY STREAM MODE: Kafka topic for JSON cdr producer")
	userDirOutput          = flag.String("path-json", "", "MANDATORY BATCH MODE: path to create json output files")
	userDirInput           = flag.String("path-cdr", "", "OPTIONAL BATCH MODE: path to cdr files (folder MUST containts CDR files only)")
	userDruidOutFile       = flag.String("druid-cdr", "", "MANDATORY BATCH MODE: CDR druid format output file")
	userSliceSize          = flag.String("slice", "", "OPTIONAL BATCH MODE: will cut resulting json file into slice of -slice <number> json lines")
	userKafkaKeyFile       = flag.String("key", "", "OPTIONAL STREAM KAFKA SSL Authentication: The optional key file for client authentication")
	userKafkaCaFile        = flag.String("ca", "", "OPTIONAL STREAM KAFKA SSL Authentication: The optional certificate authority file for TLS client authentication")
	userKafkaVerifySsl     = flag.Bool("verify", false, "OPTIONAL STREAM KAFKA SSL Authentication: Verify ssl certificates chain")
	userKafkaCertFile      = flag.String("certificate", "", "OPTIONAL STREAM KAFKA SSL Authentication: Certificate file for client authentication")
	userMicroBLoopTimeOut  = flag.String("batch-loop", "", "MANDATORY MICRO BATCH: Loop time out for micro batch in s or ms: 120s")
	userLogLevel           = flag.String("log", "", "optional - log level can be: trace debug info warn error fatal panic")
)

// End of flag declaration


func main() {
	flag.Var(&userCdrFiles, "file-cdr", "OPTIONAL BATCH MODE: List of CDR files")
	flag.Var(&userKafkaBrokerList, "kafka-ip", "MANDATORY STREAM MODE: IP address of Kafka broker")
	flag.Parse()
	if flag.NFlag() < 1 || len(*userMode) == 0 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	if len(*userLogLevel) != 0 {
		global.SetLogLevel(*userLogLevel)
		global.Logger.WithField("level", *userLogLevel).Warn("log level has been modified to this value")
	}
	switch *userMode {
	case "batch":
		{
			batchCdrMainFunc(userCdrFiles, *userDirInput, *userSliceSize,
				*userDirOutput, *userDruidOutFile, false)
			postDone, err := postBatchToDruidCluster(*userDruidOutFile, *userDirOutput, *userDruidBatchFqdn,
				*userDruidIntervals, *userDruidSchemaTpl)
			global.Logger.WithFields(logrus.Fields{
				"amount": postDone,
				"err":    err,
			}).Info("All HTTP POST Done")
		}
	case "micro-batch":
		{
			microBatchMainFunc(*userDirInput, *userDirOutput, *userDruidOutFile, *userDruidBatchFqdn,
				*userDruidSchemaTpl, *userMicroBLoopTimeOut)
		}
	case "batch-stream":
		{
			batchStreamMainFunc(*userDirInput, *userMicroBLoopTimeOut, userKafkaBrokerList,
				*userKafkaBrokerPort, *userKafkaDruidCdrTopic, *userKafkaCertFile, *userKafkaKeyFile,
				*userKafkaCaFile, *userKafkaVerifySsl)
		}
	default:
		flag.PrintDefaults()
		os.Exit(0)
	}
}

// Batch related functions

func batchCdrMainFunc(myFilesIn fileList, myDirIn string, mySliceSize string, myDirOutput string,
	myFileOut string, deleteCdrfiles bool) int {
	var totalBytes int = 0
	var bytesQty int = 0
	var errWr error
	var counter int = 0
	var amountLinePerSlice int = 0
	var errCreate error
	var sliceFileDescr *os.File

	if len(mySliceSize) != 0 {
		amountLinePerSlice, errWr = strconv.Atoi(mySliceSize)
		if errWr != nil {
			global.Logger.WithFields(logrus.Fields{
				"slice": mySliceSize,
				"err":   errWr,
			}).Fatal("Bad value for slice flag")
			return 0
		}
	}
	if (len(myFilesIn) != 0 || len(myDirIn) != 0) && len(myFileOut) != 0 && len(myDirOutput) != 0 {
		jsonFileNameOuput := fmt.Sprintf("%s/%s", myDirOutput, myFileOut)
		if myDruidCdrFileDescr, err := os.Create(string(jsonFileNameOuput)); err == nil {
			defer myDruidCdrFileDescr.Close()
			if len(myFilesIn) != 0 {
				for _, myCdrFile := range myFilesIn {
					bytesQty, errWr = cdrtools.ConvertCirpackCdrsToJsonFromFileToFileDescriptor(myCdrFile, myDruidCdrFileDescr)
					if errWr != nil {
						global.Logger.WithError(errWr).Fatal("unable to convert cdr file")
					}
				}
			} else if len(myDirIn) != 0 {
				allFilesPath, err := ioutil.ReadDir(string(myDirIn))
				if err != nil {
					global.Logger.WithError(err).Fatal("Unable to read cdr folder")
				}
				if *userMode == "micro-batch" {
					allFilesPath, err = global.CleanCdrFilesList(allFilesPath)
					if err != nil {
						global.Logger.WithError(err).Fatal("no file called work*")
					}
				}
				if len(allFilesPath) == 0 {
					global.Logger.Info("No new cdr files in folder")
					return 0
				}
				global.Logger.WithFields(logrus.Fields{
					"Amount of cdr files": len(allFilesPath),
				}).Info("Starting the cdr conversion process...")
				for _, myCdrFile := range allFilesPath {
					fullPathAndFile := fmt.Sprintf("%s/%s", string(myDirIn), myCdrFile.Name())
					bytesQty, errWr = cdrtools.ConvertCirpackCdrsToJsonFromFileToFileDescriptor(fullPathAndFile, myDruidCdrFileDescr)
					if errWr != nil {
						global.Logger.WithError(errWr).Fatal("Failed to convert cdr file")
						return 0
					}
					totalBytes += bytesQty
					// remove cdr file if requested
					if deleteCdrfiles == true {
						err := os.Remove(fullPathAndFile)
						if err != nil {
							global.Logger.WithFields(logrus.Fields{
								"File":  myCdrFile.Name(),
								"error": err,
							}).Fatal("Unable to delete cdr files after processing")
							return 0
						}
						global.Logger.WithFields(logrus.Fields{
							"deleted cdr": myCdrFile.Name(),
						}).Info("Cleaning of cdr folder")
					}
				}
			} else {
				global.Logger.WithFields(logrus.Fields{
					"Input dir": myDirIn,
				}).Fatal("Missing cdr files or path to cdr files")
				return 0
			}
			global.Logger.WithFields(logrus.Fields{
				"bytes":       totalBytes,
				"output file": jsonFileNameOuput,
			}).Info("JSON converted File written")
		}
		if amountLinePerSlice != 0 {
			if myDruidCdrFileDescr, err := os.Open(jsonFileNameOuput); err == nil {
				defer myDruidCdrFileDescr.Close()
				scanner := bufio.NewScanner(myDruidCdrFileDescr)
				for scanner.Scan() {
					if math.Mod(float64(counter), float64(amountLinePerSlice)) == 0 {
						sliceFileDescr.Close()
						sliceOutFileName := fmt.Sprintf("%s/%s-%d-%d.json", myDirOutput, myFileOut, counter, counter+amountLinePerSlice)
						sliceFileDescr, errCreate = os.Create(sliceOutFileName)
						if errCreate != nil {
							global.Logger.WithFields(logrus.Fields{
								"file name": sliceOutFileName,
							}).Fatal("Unable to create file")
						}
					}
					outputLineJson := fmt.Sprintf("%s\n", scanner.Text())
					sliceFileDescr.WriteString(outputLineJson)
					counter += 1
				}

				err := os.Remove(jsonFileNameOuput)
				if err != nil {
					global.Logger.WithError(err).Fatal("Unable to delete large json file to cut into slice")
				}
			}
		}
	} else {
		flag.PrintDefaults()
		os.Exit(0)
	}
	return totalBytes
}

func postBatchToDruidCluster(myFileOut string, myDirInput string, myDruidCluster string,
	myInterval string, myTemplate string) (int, error) {
	var listOfFiles []string
	if len(myDruidCluster) == 0 || len(myInterval) == 0 || len(myTemplate) == 0 {
		global.Logger.Warn("Invalid use of post batch function")
		return 0, fmt.Errorf("wrong parameters")
	}
	// We check if we need to post all json files of a specifc folder or if we hav just one file
	if len(myFileOut) == 0 && len(myDirInput) != 0 {
		allFilesPath, err := ioutil.ReadDir(string(myDirInput))
		if err != nil {
			global.Logger.WithError(err).Fatal("unable to read directory")
		}
		for _, myFile := range allFilesPath {
			listOfFiles = append(listOfFiles, myFile.Name())
		}
	} else if len(myFileOut) != 0 && len(myDirInput) != 0 { // here we have just one specific file to post
		listOfFiles = append(listOfFiles, myFileOut)
	} else {
		global.Logger.Warn("Invalid use of post batch function")
		return 0, fmt.Errorf("wrong parameters")
	}
	for _, myCdrFile := range listOfFiles {
		fullPathAndFile := fmt.Sprintf("%s/%s", string(myDirInput), myCdrFile)

		var baseTemplate = pongo2.Must(pongo2.FromFile(myTemplate))
		postData, err := baseTemplate.Execute(pongo2.Context{"batchContentFile": fullPathAndFile,
			"batchIntervals": myInterval})
		if err != nil {
			global.Logger.WithError(err).Panic("pongo2 template badly executed")
		}
		global.Logger.WithFields(logrus.Fields{
			"postJSON": postData,
		}).Debug("JSON request")
		druidClusterApi := fmt.Sprintf("http://%s/druid/indexer/v1/task", myDruidCluster)
		req, err := http.NewRequest("POST", druidClusterApi, strings.NewReader(postData))
		if err != nil {
			global.Logger.WithError(err).Fatal("Failed to create HTTP POST")
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			global.Logger.WithError(err).Fatal("Failed to send POST request")
		}
		defer resp.Body.Close()
		global.Logger.WithFields(logrus.Fields{
			"Response": resp,
		}).Info("HTTP POST request pushed")
	}
	return 0, nil
}


// Micro Batch related functions

func microBatchMainFunc(myDirIn string, myDirOutput string, myFileOut string, myDruidFqdn string,
	myDruidSchemaTpl string, myMicroBatchLoopTimeOut string) {
	var startBatchTime int64
	var startBatchTimeTime time.Time
	var druidIntervalStart string
	var endBatchTime int64
	var endBatchTimeTime time.Time
	var druidIntervalEnd string

	if (len(myDirIn) != 0) && len(myFileOut) != 0 &&
		len(myDirOutput) != 0 && len(myDruidFqdn) != 0 &&
		len(myDruidSchemaTpl) != 0 && len(myMicroBatchLoopTimeOut) != 0 {
		myMicroBatchLoopTimeOutDur, err := time.ParseDuration(myMicroBatchLoopTimeOut)
		if err != nil {
			global.Logger.WithError(err).Fatal("Wrong Loop time out")
		}
		startBatchTime = time.Now().Unix()
		startBatchTimeTime = time.Unix(startBatchTime, 0)
		druidIntervalStart = global.FromUnixTimeToDruid(startBatchTimeTime)
		time.Sleep(myMicroBatchLoopTimeOutDur)
		for {
			global.Logger.Print("Starting a micro batch cycle")
			// we need to get rid of the ":" to create a acceptable file name
			myFileOutWithDate := strings.Replace(druidIntervalStart, ":", "-", 2) + "-" + myFileOut
			bytesWritten := batchCdrMainFunc(nil, myDirIn, "0", myDirOutput, myFileOutWithDate, true)
			// we take the timestamp after this processing for druid interval
			// but let's sleep 2 seconds make sure we do not post info
			// with timestamp equal to end of interval
			time.Sleep(2 * time.Second)
			endBatchTime = time.Now().Unix()
			endBatchTimeTime = time.Unix(endBatchTime, 0)
			druidIntervalEnd = global.FromUnixTimeToDruid(endBatchTimeTime)
			druidInterval := fmt.Sprintf("%s/%s", druidIntervalStart, druidIntervalEnd)
			// We done the conversion into JSON let's post all JSON files to druid
			if bytesWritten != 0 {
				postDone, err := postBatchToDruidCluster(myFileOutWithDate, myDirOutput, myDruidFqdn, druidInterval, myDruidSchemaTpl)
				global.Logger.WithFields(logrus.Fields{
					"amount": postDone,
					"err":    err,
				}).Info("All HTTP POST Done")
			}
			time.Sleep(myMicroBatchLoopTimeOutDur)
			startBatchTime = time.Now().Unix()
			startBatchTimeTime = time.Unix(startBatchTime, 0)
			druidIntervalStart = global.FromUnixTimeToDruid(startBatchTimeTime)
		}
	} else {
		flag.PrintDefaults()
		os.Exit(0)
	}
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

// Functions related to micro batch cdr files pushed to Kafka

func batchStreamMainFunc(myDirInput string, myMicroBLoopTimeOut string,
	myKafkaBroker []string, myKafkaPort string, myTopic string, myKafkaCertFile string, myKafkaKeyFile string,
	myKafkaCaFile string, myKafkaVerifySsl bool) {
	// let's check if we have all we need
	if len(myDirInput) == 0 || len(myKafkaBroker) == 0 || len(myKafkaPort) == 0 ||
		len(myTopic) == 0 || len(myMicroBLoopTimeOut) == 0 {
		flag.PrintDefaults()
		global.Logger.Fatal("missing required argument for batch stream service")
	}
	myMicroBatchLoopTimeOutDur, err := time.ParseDuration(myMicroBLoopTimeOut)
	if err != nil {
		global.Logger.WithError(err).Fatal("Wrong Loop time out")
	}
	startBatchTime := time.Now().Unix()
	startBatchTimeTime := time.Unix(startBatchTime, 0)
	druidIntervalStart := global.FromUnixTimeToDruid(startBatchTimeTime)
	// create cdr producer on kafka broker
	// no need to check return as program exits if kafka fails
	var brokerListWithPort []string
	for _, myBroker := range myKafkaBroker {
		myBrokerWithPort := fmt.Sprintf("%s:%s", myBroker, myKafkaPort)
		brokerListWithPort = append(brokerListWithPort, myBrokerWithPort)
	}
	cdrProducerConfig := sarama.NewConfig()
	cdrProducerTlsConfig := createTlsConfiguration(myKafkaCertFile, myKafkaKeyFile, myKafkaCaFile,
		myKafkaVerifySsl)
	if cdrProducerTlsConfig != nil {
		cdrProducerConfig.Net.TLS.Enable = true
		cdrProducerConfig.Net.TLS.Config = cdrProducerTlsConfig
	}
	cdrProducerConfig.Producer.RequiredAcks = sarama.WaitForAll         // Only wait for the leader to ack
	cdrProducerConfig.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	cdrProducerConfig.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	// assumes we fill up the queue un in this timeout
	cdrProducerConfig.ChannelBufferSize = 4000 // Buffer of messages
	cdrProducer, err := sarama.NewAsyncProducer(brokerListWithPort, cdrProducerConfig)

	cdrProducerSignals := make(chan os.Signal, 1)
	signal.Notify(cdrProducerSignals, os.Interrupt)

	var enqueued, kafkaErrors int
	doneCh := make(chan bool, 1)
	// launch go routing to capture signal interrupt
	go func() {
		for mySig := range cdrProducerSignals {
			global.Logger.WithField("signal", mySig).Warn("signal received")
			doneCh <- true
		}
	}()

	go func() {
		for {

			time.Sleep(myMicroBatchLoopTimeOutDur)
			startBatchTime = time.Now().Unix()
			startBatchTimeTime = time.Unix(startBatchTime, 0)
			druidIntervalStart = global.FromUnixTimeToDruid(startBatchTimeTime)
			global.Logger.WithFields(logrus.Fields{
				"time": druidIntervalStart,
			}).Print("Starting a batch stream cycle")
			myCdrs, err := cdrtools.ConvertCdrFolderToCdrs(myDirInput, true)
			if err != nil {
				global.Logger.WithError(err).Fatal("issue reading/converting cdr folder")
			}
			// We  are done reading CDR's, we need to enrich, convert to JSON and send to Kafka
			for _, myCdr := range myCdrs {
				msg := &sarama.ProducerMessage{
					Topic: myTopic,
					Key:   sarama.StringEncoder("thereisnokey"),
					Value: sarama.StringEncoder(myCdr)}
				select {
				case cdrProducer.Input() <- msg:
					enqueued++
				case err := <-cdrProducer.Errors():
					kafkaErrors++
					global.Logger.WithError(err).Warn("Failed to produce message")
				}
			}
		}
	}()
	global.Logger.Info("Producer lanched ... waiting for signals")
	<-doneCh
	global.Logger.Info("Interrupt received exiting Producer loop ...")

	global.Logger.WithFields(logrus.Fields{
		"messages enqueued": enqueued,
		"errors":            kafkaErrors,
	}).Warn("exiting producer")
}

