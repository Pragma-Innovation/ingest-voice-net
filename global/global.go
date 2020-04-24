package global

import (
    "fmt"
    "github.com/sirupsen/logrus"
    "io/ioutil"
    "os"
    "strings"
    "time"
)

// variable for log

var Logger = logrus.New()

// Time variable wet when starting a new batch

var StartBatchTime int64

// init() just managing logger initialisation

func init() {
    Logger.Formatter = new(logrus.TextFormatter) // default text format
    Logger.Level = logrus.DebugLevel
}

func SetLogLevel(myLevel string) {
    var err error
    Logger.Level, err = logrus.ParseLevel(myLevel)
    if err != nil {
        Logger.WithFields(logrus.Fields{
            "log level": myLevel,
            "error":     err,
        }).Fatal("unable to set log level")
        return
    }
    return
}

// Function to get rid of the file under processing (don't touch till it is closed)
// this file is named "work*" by the cirpack micro batch tool

func CleanCdrFilesList(myFiles []os.FileInfo) ([]os.FileInfo, error) {
    returnSlice := myFiles[:0]
    for _, myFile := range myFiles {
        if !strings.Contains(myFile.Name(), "work") {
            returnSlice = append(returnSlice, myFile)
        } else {
            Logger.WithFields(logrus.Fields{
                "File removed": myFile.Name(),
            }).Info("Avoid ingesting work file")
        }
    }
    if len(myFiles) == len(returnSlice) {
        return returnSlice, fmt.Errorf("batch did not find work file in cdr folder")
    }
    return returnSlice, nil
}

// Function that parse a folder where cdr are stored
// returns a string slice with cdr files name (full path)
// it avoids taking cdr file under processing

func ReadCdrFolder(myDirInput string) ([]string, error) {
    var cdrFiles []string
    allFilesPath, err := ioutil.ReadDir(string(myDirInput))
    if err != nil {
        Logger.WithError(err).Fatal("Unable to read cdr folder")
    }
    allFilesPath, err = CleanCdrFilesList(allFilesPath)
    if err != nil {
        Logger.WithError(err).Fatal("no file called work*")
    }
    for _, myCdrFile := range allFilesPath {
        fullPathAndFile := fmt.Sprintf("%s/%s", string(myDirInput), myCdrFile.Name())
        cdrFiles = append(cdrFiles, fullPathAndFile)
    }
    return cdrFiles, nil
}

// Function summary: delete cdr files list recieved as parameter
// slice of strings with file names

func PurgeCdrFiles(myFiles []string) {
    for _, myFile := range myFiles {
        err := os.Remove(myFile)
        if err != nil {
            Logger.WithFields(logrus.Fields{
                "File":  myFile,
                "error": err,
            }).Fatal("Unable to delete cdr files after processing")
            return
        }
        Logger.WithFields(logrus.Fields{
            "deleted cdr": myFile,
        }).Info("Cleaning of cdr folder")
    }
}

func SetBatchStartTime(fileName string) {
    // we try to get time stamp out of the file name if we fail we set the time stamp ourselves
    if len(fileName) != 0 {
        fileSlices := strings.Split(fileName, "_")
        fileDate := fileSlices[len(fileSlices)-1]
        dateTimeStr := fileDate[0:4] + "-" + fileDate[4:6] +
            "-" + fileDate[6:8] + "T" + fileDate[8:10] + ":" + fileDate[10:12] + ":00Z"
        t, err := time.Parse(time.RFC3339, dateTimeStr)
        if err != nil {
            Logger.WithError(err).Warn("unable to parse cdr date and time")
            // we failed we set our own clock
            StartBatchTime = time.Now().Unix()
        }
        Logger.WithField("date", t).Debug("time stamp for batch")
        StartBatchTime = t.Unix()
    } else {
        StartBatchTime = time.Now().Unix()
    }
}

func GetBatchStartTime() int64 {
    return StartBatchTime
}
