package cdrtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/Pragma-innovation/ingest-voice-net/global"
	"github.com/Pragma-innovation/libphonego/gophonenumber"
	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// data structure for cirPack CDR (that embedded CDR's packages)
// this data structure is used to store CDR data once de serialized

type CirCdr struct {
	ackCounter     int
	AppFlag        string `"app_flag"`
	IngestTime     string `"ingest_time"`
	length         int
	amountPack     int
	cirCdrPacks    []string
	JsonEncoded    string
	LibPhoneEnrich string
}

const (
	CirCdrAckCounterOffset = 3
	CirCdrAppFlagOffset    = 8
	CirCdrDollarOffset     = 10
)

// data structure for cirpack CDR Packages
// we declare all column of CDR and associate it to its position in CDR

// structures, variable and mapping for basic package

// Cirpack format

type cirBasicPackCol int

const (
	CirBpckTLen = iota
	CirBpckAccount
	CirBpckDirection
	CirBpckCallStDate
	CirBpckCallStTime
	CirBpckConnDur
	CirBpckConnDurRg
	CirBpckTotDur
	CirBpckSwitchIp
	CirBpckAccessCode
	CirBpckTypeCallingPtAccess
	CirBpckNumPlCallingPtNum
	CirBpckCallingPtCat
	CirBpckNatCallingNum
	CirBpckCallingNum
	CirBpckNatAddCallingPtAdd
	CirBpckAddCallingPtAdd
	CirBpckAccTypeCalledNum
	CirBpckNumPlCalledPt
	CirBpckNatCalledNum
	CirBpckCalledNum
	CirBpckCatRealCalledNum
	CirBpckTypeRealCalledNum
	CirBpckNatRealCalledNum
	CirBpckRealCalledNum
	CirBpckBillingMode
	CirBpckServiceCode
	CirBpckReleaseLoc
	CirBpckOperatorId
	CirBpckCircuitId
	CirBpckInTrunkGroup
	CirBpckOutTrunkGroup
)

var cirBasicPackColNameMap = map[cirBasicPackCol]string{
	CirBpckTLen:                "type_length",
	CirBpckAccount:             "account",
	CirBpckDirection:           "direction",
	CirBpckCallStDate:          "call_start_date",
	CirBpckCallStTime:          "call_start_time",
	CirBpckConnDur:             "connection_duration",
	CirBpckConnDurRg:           "connection_ringing_duration",
	CirBpckTotDur:              "total_call_duration",
	CirBpckSwitchIp:            "switch_ip",
	CirBpckAccessCode:          "access_code",
	CirBpckTypeCallingPtAccess: "type_calling_party_access",
	CirBpckNumPlCallingPtNum:   "numbering_plan_calling_party_number",
	CirBpckCallingPtCat:        "calling_party_category",
	CirBpckNatCallingNum:       "nature_calling_number",
	CirBpckCallingNum:          "calling_number",
	CirBpckNatAddCallingPtAdd:  "nature_additional_calling_party_address",
	CirBpckAddCallingPtAdd:     "additional_calling_party_address",
	CirBpckAccTypeCalledNum:    "access_type_called_number",
	CirBpckNumPlCalledPt:       "number_plan_called_party",
	CirBpckNatCalledNum:        "nature_called_number",
	CirBpckCalledNum:           "called_number",
	CirBpckCatRealCalledNum:    "category_real_called_number",
	CirBpckTypeRealCalledNum:   "type_real_called_number",
	CirBpckNatRealCalledNum:    "nature_real_called_number",
	CirBpckRealCalledNum:       "real_called_number",
	CirBpckBillingMode:         "billing_mode",
	CirBpckServiceCode:         "service_code",
	CirBpckReleaseLoc:          "release_loc_cause",
	CirBpckOperatorId:          "operator_id",
	CirBpckCircuitId:           "circuit_id",
	CirBpckInTrunkGroup:        "in_trunk_group",
	CirBpckOutTrunkGroup:       "out_trunk_group",
}

// basic package JSON format

type jsonBasicPackCol int

const (
	JsonBpckAccount jsonBasicPackCol = iota
	JsonBpckDirection
	JsonBpckCallStartDate
	JsonBpckCallStartHour
	JsonBpckCallStartMinsec
	JsonBpckConnDur
	JsonBpckConnDurRg
	JsonBpckTotDur
	JsonBpckSwitchIp
	JsonBpckAccessCode
	JsonBpckTypeCallingPtAccess
	JsonBpckNumPlCallingPtNum
	JsonBpckCallingPtCat
	JsonBpckNatCallingNum
	JsonBpckCallingNum
	JsonBpckNatAddCallingPtAdd
	JsonBpckAddCallingPtAdd
	JsonBpckAccTypeCalledNum
	JsonBpckNumPlCalledPt
	JsonBpckNatCalledNum
	JsonBpckCalledNum
	JsonBpckCatRealCalledNum
	JsonBpckTypeRealCalledNum
	JsonBpckNatRealCalledNum
	JsonBpckRealCalledNum
	JsonBpckBillingMode
	JsonBpckServiceCode
	JsonBpckReleaseLoc
	JsonBpckOperatorId
	JsonBpckCircuitId
	JsonBpckInTrunkGroup
	JsonBpckOutTrunkGroup
)

var jsonBasicPackColNameMap = map[jsonBasicPackCol]string{
	JsonBpckAccount:             "account",
	JsonBpckDirection:           "direction",
	JsonBpckCallStartDate:       "call_start_date",
	JsonBpckCallStartHour:       "call_start_hour",
	JsonBpckCallStartMinsec:     "call_start_minsec",
	JsonBpckConnDur:             "connection_duration",
	JsonBpckConnDurRg:           "connnection_ringing_duration",
	JsonBpckTotDur:              "total_duration",
	JsonBpckSwitchIp:            "switch_ip",
	JsonBpckAccessCode:          "access_code",
	JsonBpckTypeCallingPtAccess: "type_calling_party_access",
	JsonBpckNumPlCallingPtNum:   "numbering_plan_calling_party_number",
	JsonBpckCallingPtCat:        "calling_party_category",
	JsonBpckNatCallingNum:       "nature_calling_number",
	JsonBpckCallingNum:          "calling_number",
	JsonBpckNatAddCallingPtAdd:  "nature_additional_calling_party_address",
	JsonBpckAddCallingPtAdd:     "additional_calling_party_address",
	JsonBpckAccTypeCalledNum:    "access_type_called_number",
	JsonBpckNumPlCalledPt:       "number_plan_called_party",
	JsonBpckNatCalledNum:        "nature_called_number",
	JsonBpckCalledNum:           "called_num",
	JsonBpckCatRealCalledNum:    "category_real_called_number",
	JsonBpckTypeRealCalledNum:   "type_real_called_number",
	JsonBpckNatRealCalledNum:    "nat_real_called",
	JsonBpckRealCalledNum:       "real_called_number",
	JsonBpckBillingMode:         "billing_mode",
	JsonBpckServiceCode:         "service_code",
	JsonBpckReleaseLoc:          "release_loc_cause",
	JsonBpckOperatorId:          "operator_id",
	JsonBpckCircuitId:           "circuit_id",
	JsonBpckInTrunkGroup:        "in_trunk_group",
	JsonBpckOutTrunkGroup:       "out_trunk_group",
}

// mapping table between cirpack standardized CDR and JSON/Druid CDR

var cirBasicPackColMapJsonBasicPackCol = map[cirBasicPackCol]jsonBasicPackCol{
	CirBpckAccount:             JsonBpckAccount,
	CirBpckDirection:           JsonBpckDirection,
	CirBpckConnDur:             JsonBpckConnDur,
	CirBpckConnDurRg:           JsonBpckConnDurRg,
	CirBpckTotDur:              JsonBpckTotDur,
	CirBpckAccessCode:          JsonBpckAccessCode,
	CirBpckTypeCallingPtAccess: JsonBpckTypeCallingPtAccess,
	CirBpckNumPlCallingPtNum:   JsonBpckNumPlCallingPtNum,
	CirBpckCallingPtCat:        JsonBpckCallingPtCat,
	CirBpckNatCallingNum:       JsonBpckNatCallingNum,
	CirBpckCallingNum:          JsonBpckCallingNum,
	CirBpckNatAddCallingPtAdd:  JsonBpckNatAddCallingPtAdd,
	CirBpckAddCallingPtAdd:     JsonBpckAddCallingPtAdd,
	CirBpckAccTypeCalledNum:    JsonBpckAccTypeCalledNum,
	CirBpckNumPlCalledPt:       JsonBpckNumPlCalledPt,
	CirBpckNatCalledNum:        JsonBpckNatCalledNum,
	CirBpckCalledNum:           JsonBpckCalledNum,
	CirBpckCatRealCalledNum:    JsonBpckCatRealCalledNum,
	CirBpckTypeRealCalledNum:   JsonBpckTypeRealCalledNum,
	CirBpckNatRealCalledNum:    JsonBpckNatRealCalledNum,
	CirBpckRealCalledNum:       JsonBpckRealCalledNum,
	CirBpckBillingMode:         JsonBpckBillingMode,
	CirBpckServiceCode:         JsonBpckServiceCode,
	CirBpckReleaseLoc:          JsonBpckReleaseLoc,
	CirBpckOperatorId:          JsonBpckOperatorId,
	CirBpckCircuitId:           JsonBpckCircuitId,
	CirBpckInTrunkGroup:        JsonBpckInTrunkGroup,
	CirBpckOutTrunkGroup:       JsonBpckOutTrunkGroup,
}

// column of RTP CDR cirpack package

type cirRtpPackCol int

const (
	CirRtpPackTypeLen = iota
	CirRtpPackDuration
	CirRtpPackBSent
	CirRtpPackBReceived
	CirRtpPackPckSent
	CirRtpPackPckReceived
	CirRtpPackPckLost
	CirRtpPackAvgJitter
	CirRtpPackAvgTransDelay
	CirRtpPackAddiInfo
	CirRtpPackIpAAndPort
)

// column of RTP CDR json package

type jsonRtpPackCol int

const (
	JsonRtpPackDuration = iota
	JsonRtpPackBSent
	JsonRtpPackBReceived
	JsonRtpPackPckSent
	JsonRtpPackPckReceived
	JsonRtpPackPckLost
	JsonRtpPackAvgJitter
	JsonRtpPackAvgTransDelay
	JsonRtpPackAddiInfo
	JsonRtpPackIp
	JsonRtpPackPort
)

var jsonRtpPackColNameMap = map[jsonRtpPackCol]string{
	JsonRtpPackDuration:      "rtp_duration",
	JsonRtpPackBSent:         "rtp_bytes_sent",
	JsonRtpPackBReceived:     "rtp_bytes_received",
	JsonRtpPackPckSent:       "rtp_pck_sent",
	JsonRtpPackPckReceived:   "rtp_pck_received",
	JsonRtpPackPckLost:       "rtp_pck_lost",
	JsonRtpPackAvgJitter:     "rtp_avg_jitter",
	JsonRtpPackAvgTransDelay: "rtp_avg_trans_delay",
	JsonRtpPackAddiInfo:      "rtp_addi_info",
	JsonRtpPackIp:            "rtp_ip",
	JsonRtpPackPort:          "rtp_port",
}

var cirRtpPackColMapJsonRtpPackCol = map[cirRtpPackCol]jsonRtpPackCol{
	CirRtpPackDuration:      JsonRtpPackDuration,
	CirRtpPackBSent:         JsonRtpPackBSent,
	CirRtpPackBReceived:     JsonRtpPackBReceived,
	CirRtpPackPckSent:       JsonRtpPackPckSent,
	CirRtpPackPckReceived:   JsonRtpPackPckReceived,
	CirRtpPackPckLost:       JsonRtpPackPckLost,
	CirRtpPackAvgJitter:     JsonRtpPackAvgJitter,
	CirRtpPackAvgTransDelay: JsonRtpPackAvgTransDelay,
	CirRtpPackAddiInfo:      JsonRtpPackAddiInfo,
}

func ConvertCirCdrFileToDruidCdrFile(myCdrFile string, myOutPutFile *os.File) (int, error) {
	// open a file
	var writtenBytes int = 0
	var errWt error
	if myCdrFileDesc, err := os.Open(myCdrFile); err == nil {
		// make sure it gets closed
		defer myCdrFileDesc.Close()

		// create a new scanner and read the file line by line
		scanner := bufio.NewScanner(myCdrFileDesc)
		for scanner.Scan() {
			err, jsonCdrBytes := convertStringCdrToStreamCdr(scanner.Text())
			if err != nil {
				global.Logger.WithError(err).Warn("unable to convert a string cdr to a byte stream")
			}
			fileTime, err := convertCdrFileNameToTimeStamp(myCdrFile)
			if err != nil {
				return -1, fmt.Errorf("unbale to convert cdr file name to timestamp")
			}
			err, jsonCdrs := DeserializerCirCdr(jsonCdrBytes, fileTime)
			for _, jsonCdr := range jsonCdrs {
				// jsonCdr.CdrPrint()
				err := jsonCdr.EnrichWibLibPhone()
				if err != nil {
					global.Logger.WithError(err).Warn("Unable to enrich cdr")
				}
				err = jsonCdr.JsonEncode()
				if err != nil {
					global.Logger.WithFields(logrus.Fields{
						"buggy cdr": jsonCdr,
					}).Warn("Unable to json marshal CDR")
				}
				writtenBytes, errWt = myOutPutFile.WriteString(jsonCdr.JsonEncoded)
				if errWt != nil {
					return 0, errWt
				}
			}

		}
		// check for errors
		if err = scanner.Err(); err != nil {
			global.Logger.WithError(err).Fatal("unable to scan cdr files line by line")
		}

	} else {
		global.Logger.WithError(err).Fatal("Unable to open JSON cdr file")
	}
	return writtenBytes, nil
}

// This function is reading the content of a CDR folder (produce by cirpack cdr tool)
// Read CDR files and return a CirCdr slice (structured CDR)
func ConvertCdrFolderToCdrs(myCdrDirInput string, purgeCdrFiles bool) ([]CirCdr, error) {
	var returnCdrs []CirCdr

	myCdrFiles, err := global.ReadCdrFolder(myCdrDirInput)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to read cdr folder")
	}
	if len(myCdrFiles) == 0 {
		global.Logger.Info("CDR folder is empty waiting for next batch cycle")
		return nil, nil
	}
	for _, cdrFile := range myCdrFiles {
		fmt.Printf("test file: %s\n", cdrFile)
	}
	for _, cdrFile := range myCdrFiles {
		if myCdrFileDesc, err := os.Open(cdrFile); err == nil {
			// make sure it gets closed
			defer myCdrFileDesc.Close()

			// create a new scanner and read the file line by line
			scanner := bufio.NewScanner(myCdrFileDesc)
			for scanner.Scan() {
				err, jsonCdrBytes := convertStringCdrToStreamCdr(scanner.Text())
				if err != nil {
					global.Logger.WithError(err).Warn("Unable to convert a string cdr to a byte stream")
				}
				fileTime, err := convertCdrFileNameToTimeStamp(cdrFile)
				if err != nil {
					return nil, fmt.Errorf("unbale to convert cdr file name to timestamp")
				}
				err, jsonCdrs := DeserializerCirCdr(jsonCdrBytes, fileTime)
				// Let's put together all cdr's into the same slice
				returnCdrs = append(returnCdrs, jsonCdrs...)

				if err = scanner.Err(); err != nil {
					global.Logger.WithError(err).Fatal("unable to scan cdr files line by line")
				}
			}
		}
	}
	global.Logger.WithFields(logrus.Fields{
		"cdr files": len(myCdrFiles),
		"cdr's":     len(returnCdrs),
	}).Info("Cdr's extracted from folder")
	if purgeCdrFiles {
		global.PurgeCdrFiles(myCdrFiles)
	}
	return returnCdrs, nil
}

// Function summary: take a cirpack CDR string as input and return a slide of
// strings that match the JSON cdr format
func convertCirBasicPackToJsonStringSlice(myCirBasicPack string) ([]string, error) {
	var splittedDruidBasicPack []string
	splittedCirBasicPack := strings.Split(myCirBasicPack, " ")
	// we allocate a slice of the size of the json pack mapping table (table of keys)
	splittedDruidBasicPack = make([]string, len(jsonBasicPackColNameMap))
	// Let's copy fields for which we have a 1 to 1 binding with cirpack package
	for index := 0; index < len(splittedCirBasicPack); index++ {
		// we check if we have a mapping between cirpack column and CDR json Keys
		if _, isPresent := cirBasicPackColMapJsonBasicPackCol[cirBasicPackCol(index)]; isPresent {
			splittedDruidBasicPack[cirBasicPackColMapJsonBasicPackCol[cirBasicPackCol(index)]] = splittedCirBasicPack[index]

		}
	}
	// now let's deal with specific fields
	// timestamp in CDR to be converted into druid key values
	timeDate, timeHour, timeMinSec, err := convertCirBasicPackTimeToDruidTimeStamp(splittedCirBasicPack[CirBpckCallStDate], splittedCirBasicPack[CirBpckCallStTime])
	if err == nil {
		splittedDruidBasicPack[JsonBpckCallStartDate] = timeDate
		splittedDruidBasicPack[JsonBpckCallStartHour] = timeHour
		splittedDruidBasicPack[JsonBpckCallStartMinsec] = timeMinSec
	} else {
		global.Logger.WithError(err).Warn("Unable to convert CDR timestamp to JSON format")
		return nil, err
	}
	// the IP address of the switch to be converted from hexa to decimal ip address
	ipAddress, _ := convertHexaIpToStringIp(splittedCirBasicPack[CirBpckSwitchIp])
	splittedDruidBasicPack[JsonBpckSwitchIp] = ipAddress
	return splittedDruidBasicPack, nil
}

// Function summary: take a cirpack CDR string as input and return a slide of
// strings that match the JSON cdr format
func convertCirRtpPackToJsonStringSlice(myCirRtpPack string) ([]string, error) {
	var splittedJsonRtpPack []string
	splittedCirRtpPack := strings.Split(myCirRtpPack, " ")
	// we allocate a slice of the size of the json rtp pack mapping table (table of keys)
	splittedJsonRtpPack = make([]string, len(jsonRtpPackColNameMap))
	// Let's copy fields for which we have a 1 to 1 binding with cirpack rtp package
	for index := 0; index < len(splittedCirRtpPack); index++ {
		// we check if we have a mapping between cirpack column and CDR json Keys
		if _, isPresent := cirRtpPackColMapJsonRtpPackCol[cirRtpPackCol(index)]; isPresent {
			splittedJsonRtpPack[cirRtpPackColMapJsonRtpPackCol[cirRtpPackCol(index)]] = splittedCirRtpPack[index]
		}
	}
	splittedIpAndPort := strings.Split(splittedCirRtpPack[CirRtpPackIpAAndPort], ":")
	splittedJsonRtpPack[JsonRtpPackIp] = splittedIpAndPort[0]
	splittedJsonRtpPack[JsonRtpPackPort] = splittedIpAndPort[1]
	return splittedJsonRtpPack, nil
}

// Function summary: take a CDR basic package in string format and convert it
// to a druid CDR JSON format as returned string
func (myCdr *CirCdr) convertCirBasicPackToJson() (string, error) {
	var druidBasicPack string
	splittedDruidBasicPack, err := convertCirBasicPackToJsonStringSlice(myCdr.cirCdrPacks[0])
	if err != nil {
		global.Logger.WithError(err).Warn("Unable to convert CDR std pack to druid slice")
		return "", err
	}
	// conversion into JSON format
	for index := 0; index < len(jsonBasicPackColNameMap); index++ {
		if jsonBasicPackCol(index) == JsonBpckConnDur || jsonBasicPackCol(index) == JsonBpckConnDurRg ||
			jsonBasicPackCol(index) == JsonBpckTotDur {
			druidBasicPack = fmt.Sprintf("%s\"%s\": %s, ", druidBasicPack,
				jsonBasicPackColNameMap[jsonBasicPackCol(index)], splittedDruidBasicPack[index])
		} else {
			druidBasicPack = fmt.Sprintf("%s\"%s\": \"%s\", ", druidBasicPack,
				jsonBasicPackColNameMap[jsonBasicPackCol(index)], splittedDruidBasicPack[index])
		}
	}
	druidBasicPack = strings.TrimSuffix(druidBasicPack, ", ")
	return druidBasicPack, nil
}

// Function summary: take a CDR basic package in string format and convert it
// to a druid CDR JSON format as returned string
func (myCdr *CirCdr) convertCirOptionalPackToJson() (string, error) {
	var endResult string
	for _, myPack := range myCdr.cirCdrPacks {
		switch myPack[0] {
		case 'T':
			// std package we do nothing
		case 'S':
			// Service node package
		case 'H':
			// Charging source package
		case 'M':
			// Multiple redirection package
		case 'R':
			// Reselection package
		case 'G':
			// Time Package
		case 'K':
			// Call Name package
		case 'D':
			// Call reference package
		case 'V':
			// Codec package
		case 'O':
			// RTP package
			endResult = convertRtpPackIntoJson(myPack)
		case 'E':
			// IE list package
		case 'L':
			// Location number package
		case 'Z':
			// SIP call id package
		case 'W':
			// Associated Call reference package
		case 'B':
			// Original call reference package
		case 'N':
			// MSRN package
		case 'U':
			// Type user package
		case 'Q':
			// Out connect package
		case 'X':
			// P charging vectors package
		case 'Y':
			// Company and group ID package
		case 'A':
			// Additional cost package
		case 'f':
			// SIP forking package
		default:
			global.Logger.WithField("pack", myPack).Warn("unmanaged package")
		}
	}
	return endResult, nil
}

func convertRtpPackIntoJson(myCirRtpPack string) string {
	var jsonRtpPack string
	rtpPackSlice, _ := convertCirRtpPackToJsonStringSlice(myCirRtpPack)
	// we loop on all package fields and add double quote when it is non metric value
	// for strings we add double quote
	for index := 0; index < len(rtpPackSlice); index++ {
		if jsonRtpPackCol(index) == JsonRtpPackAddiInfo || jsonRtpPackCol(index) == JsonRtpPackIp ||
			jsonRtpPackCol(index) == JsonRtpPackPort {
			jsonRtpPack = jsonRtpPack + "\"" + jsonRtpPackColNameMap[jsonRtpPackCol(index)] +
				"\": \"" + rtpPackSlice[index] + "\", "
		} else {
			jsonRtpPack = jsonRtpPack + "\"" + jsonRtpPackColNameMap[jsonRtpPackCol(index)] +
				"\": " + rtpPackSlice[index] + ", "
		}

	}
	jsonRtpPack = strings.TrimSuffix(jsonRtpPack, ", ")
	return jsonRtpPack
}

// Function Summary: Takes Cirpack CDR time stamp information, parse it and returns it into pieces
// date, hour, minute-seconds (all in strings)

func convertCirBasicPackTimeToDruidTimeStamp(myCallDate string, myCallTime string) (string, string, string, error) {
	if len(myCallDate) != 8 {
		global.Logger.WithFields(logrus.Fields{
			"call date": myCallDate,
		}).Warn("Call date badly formatted unable to convert into JSON")
		err := fmt.Errorf("call Date field badly formatted %s", myCallDate)
		return "", "", "", err
	}
	myYear := myCallDate[0:4]
	myMonth := myCallDate[4:6]
	myDay := myCallDate[6:8]
	// as timestamp is critical value
	// let's do a sanity check of date value
	if i, err := strconv.Atoi(myYear); err != nil || (i < 1900 && i > 2999) {
		err := fmt.Errorf("year badly formatted %s", myYear)
		return "", "", "", err
	}
	if i, err := strconv.Atoi(myMonth); err != nil || (i < 1 && i > 12) {
		err := fmt.Errorf("month badly formatted %s", myMonth)
		return "", "", "", err
	}
	if i, err := strconv.Atoi(myDay); err != nil || (i < 1 && i > 31) {
		err := fmt.Errorf("day badly formatted %s", myDay)
		return "", "", "", err
	}
	myHours := myCallTime[0:2]
	myMins := myCallTime[2:4]
	mySecs := myCallTime[4:6]
	// let's do some sanity check of the time value
	if i, err := strconv.Atoi(myHours); err != nil || (i > 24) {
		err := fmt.Errorf("hour in time badly formatted %s", myHours)
		return "", "", "", err
	}
	if i, err := strconv.Atoi(myMins); err != nil || (i > 59) {
		err := fmt.Errorf("mimutes in time badly formatted %s", myMins)
		return "", "", "", err
	}
	if i, err := strconv.Atoi(mySecs); err != nil || (i > 59) {
		err := fmt.Errorf("seconds in time badly formatted %s", mySecs)
		return "", "", "", err
	}
	// looks good let's format java style time stamp
	return fmt.Sprintf("%s-%s-%s", myYear, myMonth, myDay),
		fmt.Sprintf("%s", myHours),
		fmt.Sprintf("%s-%s", myMins, mySecs), nil
}

// Function summary: it convert the IP address received as a string
// with IP address in hexa into a string with IP address in decimal

func convertHexaIpToStringIp(myHexaIp string) (string, error) {
	var ipAddress [4]int64
	var indexIp int
	for index := 0; index < 8; index += 2 {
		indexIp = index / 2
		myByte, _ := strconv.ParseInt(myHexaIp[index:index+2], 16, 32)
		ipAddress[indexIp] = myByte
	}
	return fmt.Sprintf("%d.%d.%d.%d", ipAddress[0], ipAddress[1], ipAddress[2], ipAddress[3]), nil
}

// Function summary: take a slice of bytes that contains circpack CDRs
// and convert it into a slice of CirCdr structures

func DeserializerCirCdr(myBuffer []byte, myOptionalTime string) (error, []CirCdr) {
	var index = 0
	var cirPackIndex = 0
	var returnedCirCdrs []CirCdr
	var currentCirCdr CirCdr
	var currentPack string
	var jsonTime string
	if len(myBuffer) == 0 {
		global.Logger.WithField("buffer len", "0").Warn("CDR deserializer received an empty buffer")
		err := fmt.Errorf("CDR deserializer received an empty buffer")
		return err, nil
	}
	if len(myOptionalTime) == 0 {
		currentTime := time.Now().Unix()
		currentTimeString := time.Unix(currentTime, 0)
		jsonTime = global.FromUnixTimeToDruid(currentTimeString)
	} else {
		jsonTime = myOptionalTime
	}
	for index < len(myBuffer) {
		currentCirCdr.length = 0
		currentCirCdr.AppFlag = ""
		currentCirCdr.IngestTime = jsonTime
		currentCirCdr.ackCounter = 0
		currentCirCdr.amountPack = 0
		currentCirCdr.cirCdrPacks = nil
		// let's create a byte io buffer for binary readings
		bytesBuf := bytes.NewBuffer(myBuffer)
		// let's check that we have the whole header we need
		if index+15 >= len(myBuffer) {
			global.Logger.WithField("index", index).Warn("We don't have the full cdr header")
			return nil, returnedCirCdrs
		}
		// let's inspect this buf and check if we have the first 8 bytes
		// the $ anchor cdr type and cdr length
		checkDollar := rune(myBuffer[CirCdrDollarOffset+index])
		if checkDollar != '$' {
			global.Logger.WithFields(logrus.Fields{
				"index": index,
				"byte":  checkDollar,
			}).Warn("CDR badly formatted dropping the whole segment")
			break
		}
		// looks like we face a properly formatted CDR header
		// let's position buffer on next coming cdr
		bytesBuf.Next(index)
		bytesBuf.Next(CirCdrAckCounterOffset)
		var intTempo uint16 = 0
		var readError error
		readError = binary.Read(bytesBuf, binary.BigEndian, &intTempo)
		if readError != nil {
			global.Logger.WithError(readError).Warn("binary Read failed")
			return readError, nil
		}
		bytesBuf.Next(CirCdrAppFlagOffset - (CirCdrAckCounterOffset + 2))
		currentCirCdr.ackCounter = int(intTempo)
		currentCirCdr.AppFlag, readError = bytesBuf.ReadString('$')
		if readError != nil {
			global.Logger.WithError(readError).Warn("Issue reading cdr App Flag")
			return readError, nil
		}
		// remove trailing $
		currentCirCdr.AppFlag = strings.TrimRight(currentCirCdr.AppFlag, "$")
		var cdrLenAsString string
		cdrLenAsString, readError = bytesBuf.ReadString(' ')
		if readError != nil {
			global.Logger.WithError(readError).Warn("Issue reading cdr length")
			return readError, nil
		}
		cdrLenAsString = strings.TrimSpace(cdrLenAsString)
		tempoLen, convErr := strconv.ParseInt(cdrLenAsString, 16, 32)
		if convErr != nil {
			global.Logger.WithError(convErr).Warn("Issue while converting cdr length")
			return convErr, nil
		}
		currentCirCdr.length = int(tempoLen)
		// we check we do have he whole cdr in buffer
		if currentCirCdr.length > len(myBuffer)-index {
			global.Logger.WithField("CDR length", currentCirCdr.length).Warn("We don't have the full cdr payload")
			return nil, returnedCirCdrs
		}
		cirPackIndex = 15                           // we start reading package after the header fix size of 15 bytes
		for cirPackIndex < currentCirCdr.length-4 { // 4 mysterious bytes as trailer
			currentPack = ""
			var packType byte
			readError = binary.Read(bytesBuf, binary.BigEndian, &packType)
			if readError != nil {
				global.Logger.WithError(readError).Warn("unable to read package type")
				return readError, nil
			}
			var packLenAsString string
			packLenAsString, readError = bytesBuf.ReadString(' ')
			if readError != nil {
				global.Logger.WithError(readError).Warn("Issue reading pack length")
				return readError, nil
			}
			packLenAsString = strings.TrimSpace(packLenAsString)
			tempoLen, convErr := strconv.ParseInt(packLenAsString, 16, 32)
			if convErr != nil {
				global.Logger.WithError(convErr).Warn("Issue while converting pack length")
				return convErr, nil
			}
			packLength := int(tempoLen)
			remainingBytes := bytesBuf.Next(packLength)
			currentPack = fmt.Sprintf("%s%s%s%s", string(packType), packLenAsString,
				" ", string(remainingBytes[0:packLength-1])) // we remove trailer " " or 0x00
			currentCirCdr.cirCdrPacks = append(currentCirCdr.cirCdrPacks, currentPack)
			cirPackIndex = cirPackIndex + packLength + 5 // 5 bytes = 1 pack type , 3 len, 1 delimiter
			currentCirCdr.amountPack++
		}
		bytesBuf.Next(4) // reading the 4 bytes as trailer
		returnedCirCdrs = append(returnedCirCdrs, currentCirCdr)
		index = index + currentCirCdr.length + 13 // header 14 bytes but len is one byte too long (cirpack bug ?)
	}
	return nil, returnedCirCdrs
}

// Function summary: there is a small difference between CDR in files and CDR's
// received from a TCP socket or a pipe. This function convert a CDR received from a file
// into a CDR as received from a socket

func convertStringCdrToStreamCdr(myCdrStr string) (error, []byte) {

	if len(myCdrStr) == 0 {
		global.Logger.Warn("cannot convert an empty CDR string")
		err := fmt.Errorf("cannot convert an empty cdr")
		return err, nil
	}
	resultBytes := make([]byte, len(myCdrStr)+8+5) // adding the 8 bytes header and 5 bytes trailer
	copy(resultBytes[8:], myCdrStr)
	return nil, resultBytes
}

// CDR methods

func (myCdr *CirCdr) CdrPrint() {
	fmt.Println("================")
	fmt.Println("Cdr ack counter: ", myCdr.ackCounter, " CDR app flag: ", myCdr.AppFlag, " length: ", myCdr.length)
	fmt.Println("Amount of packages: ", myCdr.amountPack)
	for _, myPack := range myCdr.cirCdrPacks {
		fmt.Println("pack: ", myPack)
	}
	fmt.Println("++++++++++++++++")
}

// Method summary: send the JSON format of the CDR to Kafka producer

func (myCdr *CirCdr) Send(myCdrProducer sarama.AsyncProducer, myTopic string) {
	if len(myCdr.JsonEncoded) == 0 {
		global.Logger.Warn("Cannot send empty JSON cdr to Kafka")
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: myTopic,
		Key:   nil,
		Value: sarama.StringEncoder(myCdr.JsonEncoded)}
	myCdrProducer.Input() <- msg
}

// Method summary: Add the JSON format of the CDR (key/values) in the CDR structure

func (myCdr *CirCdr) JsonEncode() error {
	// initialize string to ""
	myCdr.JsonEncoded = ""
	cdrAppTag := reflect.TypeOf(*myCdr).Field(1).Tag
	appFlagKeyValue := string(cdrAppTag) + ": \"" + myCdr.AppFlag + "\""
	cdrTimeStamp := reflect.TypeOf(*myCdr).Field(2).Tag
	timestampKeyValue := string(cdrTimeStamp) + ": \"" + myCdr.IngestTime + "\""
	basicPackJson, err := myCdr.convertCirBasicPackToJson()
	if err != nil {
		global.Logger.WithError(err).Warn("unable to convert cdr std package in JSON")
		return err
	}
	optionalPack, err := myCdr.convertCirOptionalPackToJson()
	if err != nil {
		global.Logger.WithError(err).Warn("unable to convert cdr optional packages in JSON")
		return err
	}
	myCdr.JsonEncoded = "{" + appFlagKeyValue + ", " + timestampKeyValue + ", " + basicPackJson
	if len(optionalPack) != 0 {
		myCdr.JsonEncoded = myCdr.JsonEncoded + ", " + optionalPack
	}
	if len(myCdr.LibPhoneEnrich) != 0 {
		myCdr.JsonEncoded = myCdr.JsonEncoded + ", " + myCdr.LibPhoneEnrich
	}
	myCdr.JsonEncoded = myCdr.JsonEncoded + "}\n"
	return nil
}

// Method summary: This method is calling the libphonenumber to enrich information
// within the CDR structure geoloc/analysis... of called number

func (myCdr *CirCdr) EnrichWibLibPhone() error {
	if len(myCdr.cirCdrPacks) == 0 {
		global.Logger.Warn("Cannot enrich an empty cirpack standard package")
		return fmt.Errorf("empty cirpack std package")
	}
	myRealCalled, myNatOfRealCalled, err := getStandardCalledNumFromCirCdrPack(myCdr.cirCdrPacks[0])
	if err != nil {
		global.Logger.WithError(err).Warn("Error creating E164 number, doing the best we can ... ")
	}
	switch myNatOfRealCalled {
	case "2", "0":
		{
			myCdr.LibPhoneEnrich = fmt.Sprintf("\"called_country_code\": \"unknown code\", \"called_country\": \"ZZ\", \"called_number_type\": \"unknown type\", \"called_number_location\": \"unknown location\"")
			return nil
		}
	case "115":
		{
			myCdr.LibPhoneEnrich = fmt.Sprintf("\"called_country_code\": \"33\", \"called_country\": \"FR\", \"called_number_type\": \"short num\", \"called_number_location\": \"unknown location\"")
			return nil
		}
	default:
		// removing too buggy called number before calling the libphonenumber
		if !strings.Contains(myRealCalled, "+0") {
			myCdr.LibPhoneEnrich = phonego.GoPgmPtsnColumnFromCalledNum(myRealCalled)
		}
	}
	return nil

}

// Function summary: this function take a CDR in a string format (unparsed) and
// provide a E164 format of the called number in the CDR
func getStandardCalledNumFromCirCdrPack(myCirCdrPack string) (string, string, error) {
	if len(myCirCdrPack) == 0 {
		global.Logger.Warn("Cannot enrich an empty cirpack standard package")
		return "", "", fmt.Errorf("empty parameter")
	}
	splittedDruidBasicPack, err := convertCirBasicPackToJsonStringSlice(myCirCdrPack)
	if err != nil {
		global.Logger.WithError(err).Warn("Unable to convert CDR std pack to json slice")
		return "", "", err
	}
	switch splittedDruidBasicPack[JsonBpckNatRealCalledNum] {
	case "3": // national call
		if strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "262") ||
		strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "692") ||
		strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "693") {
			return "+262" + splittedDruidBasicPack[JsonBpckRealCalledNum], splittedDruidBasicPack[JsonBpckNatRealCalledNum], nil
		} else if strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "590") ||
			strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "690") {
			return "+590" + splittedDruidBasicPack[JsonBpckRealCalledNum], splittedDruidBasicPack[JsonBpckNatRealCalledNum], nil
		} else if strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "594") ||
			strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "694") {
			return "+594" + splittedDruidBasicPack[JsonBpckRealCalledNum], splittedDruidBasicPack[JsonBpckNatRealCalledNum], nil
		} else if strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "596") ||
			strings.HasPrefix(splittedDruidBasicPack[JsonBpckRealCalledNum], "696") {
			return "+596" + splittedDruidBasicPack[JsonBpckRealCalledNum], splittedDruidBasicPack[JsonBpckNatRealCalledNum], nil
		} else {
			return "+33" + splittedDruidBasicPack[JsonBpckRealCalledNum], splittedDruidBasicPack[JsonBpckNatRealCalledNum], nil
		}
	case "4": // international calls
		return "+" + splittedDruidBasicPack[JsonBpckRealCalledNum], splittedDruidBasicPack[JsonBpckNatRealCalledNum], nil
	case "2":
		return "", splittedDruidBasicPack[JsonBpckNatRealCalledNum], fmt.Errorf("libphone cannot deal with unknown number")
	case "115":
		return "", splittedDruidBasicPack[JsonBpckNatRealCalledNum], fmt.Errorf("libphone cannot deal with short number")
	default:
		global.Logger.WithField("nature of real called", splittedDruidBasicPack[JsonBpckNatRealCalledNum]).Warn("unknown nature")
		return "", splittedDruidBasicPack[JsonBpckNatRealCalledNum], fmt.Errorf("nature of real called unknown")
	}
	return "", "", fmt.Errorf("error function getStandardCalledNumFromCirCdrPack")
}

func convertCdrFileNameToTimeStamp(myFileName string) (string, error){
	splittedFileName := strings.Split(myFileName, "_")
	if len(splittedFileName) == 0 {
		return "", fmt.Errorf("empty file name")
	}
	myFileTime := splittedFileName[len(splittedFileName)-1]
	myParsedTime := myFileTime[0:4] + "-" + myFileTime[4:6] + "-" + myFileTime[6:8] + "T" + myFileTime[8:10] + ":" +
		myFileTime[10:12] + ":00.000Z"
	return myParsedTime, nil
}