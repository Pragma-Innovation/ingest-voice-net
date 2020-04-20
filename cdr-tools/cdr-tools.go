package cdrtools

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/sirupsen/logrus"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Data structure for handling Cirpack CDR
// concept of Blocks has been introduced to manage the first step where we
// deserialize pieces (blocks) of the CDR without getting into packages fields

type PackageBlock struct {
	Type    string
	Length  int
	Payload string
}

type CirpackCdrBlocks struct {
	IngestTime  int64
	Header      string
	Length      int
	PackagesQty int
	Packages    []*PackageBlock
}

// CDR fully structured

// Header

type CirpackHeader struct {
	AppFlag string `json:"app_flag"`
	Length  int    `json:"length"`
}

// Interface that group together all optional packages having the same methods

type CirpackPackage interface {
	FulfilCdrTsJson(cdrTsModel *CirpackCdrJsonTimeSeries) error
	LoadCdrFieldsFromBlockPayload(payload string) error
}

type CirpackCdr struct {
	IngestTime int64
	Header     CirpackHeader
	Packages   []CirpackPackage
}

type CirpackUndefinedPackage struct{}

type CirpackBasicPackage struct {
	Account                             string `json:"account"`
	Direction                           string `json:"direction"`
	CallStartDate                       string `json:"call_start_date"`
	CallStartTime                       string `json:"call_start_time"`
	ConnectionDuration                  uint   `json:"connection_duration"`
	ConnectionRingingDuration           uint   `json:"connnection_ringing_duration"`
	TotalDuration                       uint   `json:"total_duration"`
	SwitchIp                            string `json:"switch_ip"`
	AccessCode                          string `json:"access_code"`
	TypeCallingPartyAccess              string `json:"type_calling_party_access"`
	NumberPlanCallingPartyNumber        string `json:"numbering_plan_calling_party_number"`
	CallingPartyCategory                string `json:"calling_party_category"`
	NatureCallingNumber                 string `json:"nature_calling_number"`
	CallingNumber                       string `json:"calling_number"`
	NatureAdditionalCallingPartyAddress string `json:"nature_additional_calling_party_address"`
	AdditionalCallingPartyAddress       string `json:"additional_calling_party_address"`
	AccessTypeCalledNumber              string `json:"access_type_called_number"`
	NumberPlanCalledParty               string `json:"number_plan_called_party"`
	NatureCalledNumber                  string `json:"nature_called_number"`
	CalledNumber                        string `json:"called_number"`
	CategoryRealCalledNumber            string `json:"category_real_called_number"`
	TypeRealCalledNumber                string `json:"type_real_called_number"`
	NatRealCalled                       string `json:"nat_real_called"`
	RealCalledNumber                    string `json:"real_called_number"`
	BillingMode                         string `json:"billing_mode"`
	ServiceCode                         string `json:"service_code"`
	ReleaseLocCause                     string `json:"release_loc_cause"`
	OperatorId                          string `json:"operator_id"`
	CircuitId                           string `json:"circuit_id"`
	InTrunkGroup                        string `json:"in_trunk_group"`
	OutTrunkGroup                       string `json:"out_trunk_group"`
	Units                               uint   `json:"charge_units"`
}

type CirpackRtpPackage struct {
	RtpDuration       uint   `json:"rtp_duration"`
	RtpBytesSent      uint   `json:"rtp_bytes_sent"`
	RtpBytesReceived  uint   `json:"rtp_bytes_received"`
	RtpPacketSent     uint   `json:"rtp_pck_sent"`
	RtpPacketReceived uint   `json:"rtp_pck_received"`
	RtpPacketLost     uint   `json:"rtp_pck_lost"`
	RtpAvgJitter      uint   `json:"rtp_avg_jitter"`
	RtpAvgTransDelay  uint   `json:"rtp_avg_trans_delay"`
	RtpAddiInfo       string `json:"rtp_addi_info"`
	RtpIpPort         string `json:"rtp_ip_and_port"`
}

// Tools used for decoding

// Convert a string that gives a length in hexadecimal into an int

func convertHexaLentghStrToInt(hexaStr string) (int, error) {
	tempoLen, err := strconv.ParseInt(hexaStr, 16, 32)
	if err != nil {
		global.Logger.WithError(err).Warn("conversion issue")
		return -1, fmt.Errorf("issue while converting hexa string to int")
	}
	return int(tempoLen), nil
}

// Read CDR package from a string that contains remaining data that is still unread
// return package type (string), package length (int), payload (string), remaining data (string, error if any

func readCirpackPackage(remainingData string) (string, int, string, string, error) {
	var packageType, packagePayload, remainingString string
	var packageLen int
	if len(remainingData) == 0 {
		err := fmt.Errorf("trying to read an empty string as package")
		return "", -1, "", "", err
	}
	packageType = remainingData[0:1]
	packLenStrStart := remainingData[1:]
	spaceIndex := strings.Index(packLenStrStart, " ")
	packLenStr := packLenStrStart[0:spaceIndex]
	var err error
	packageLen, err = convertHexaLentghStrToInt(packLenStr)
	if err != nil {
		err := fmt.Errorf("issue while decoding header cdr length")
		return "", -1, "", "", err
	}
	// we add 1 to skip space character
	packPayloadStart := packLenStrStart[spaceIndex+1:]
	// len + 1 as payload length include separator space character
	if len(packPayloadStart)+1 < packageLen {
		err := fmt.Errorf("package is probably trunkated or malformed")
		return "", -1, "", "", err
	}
	// packageLen -1 as we already removed the space separator between package header and payload
	packagePayload = packPayloadStart[:packageLen-1]
	if packageLen == len(packPayloadStart)+1 {
		remainingString = ""
	} else {
		remainingString = packPayloadStart[packageLen:]
	}
	return packageType, packageLen, packagePayload, remainingString, nil
}

func splitCdrHeader(header string) []string {
	if strings.Contains(header, "$") {
		// CDR case
		return strings.Split(header, "$")
	} else if strings.Contains(header, "@") {
		// iCDR case
		return strings.Split(header, "@")
	} else {
		global.Logger.WithFields(logrus.Fields{
			"header": header,
		}).Warn("invalid header")
		return []string{}
	}
}

// Methods of CirpackCdrBlocks
// All read methods return remaining data (string) to be read and a potential error

func (cdrBlocks *CirpackCdrBlocks) ReadHeaderBlock(cdrString string) (string, error) {
	headerLen := strings.IndexRune(cdrString, ' ')
	header := cdrString[0:headerLen]
	headerSlice := splitCdrHeader(header)
	if len(headerSlice) <= 1 {
		return "", fmt.Errorf("cdr header badly formatted : wrong header")
	}
	lenAsStrHex := headerSlice[1]
	var err error
	cdrBlocks.Length, err = convertHexaLentghStrToInt(lenAsStrHex)
	if err != nil {
		global.Logger.WithError(err).Warn("issue while decoding header cdr length")
		return "", err
	}
	cdrBlocks.Header = header
	return cdrString[7:], nil
}

func (cdrBlocks *CirpackCdrBlocks) ReadPackagesBlocks(cdrData string) error {
	var packType, packPayload string
	var packLen int
	var err error
	remainingData := cdrData
	for len(remainingData) != 0 {
		packType, packLen, packPayload, remainingData, err =
			readCirpackPackage(remainingData)
		if err != nil {
			global.Logger.WithFields(logrus.Fields{
				"data": remainingData,
			}).Debug("issue while reading optional package block")
			return err
		}
		cdrBlocks.Packages =
			append(cdrBlocks.Packages, &PackageBlock{
				Type:    packType,
				Length:  packLen,
				Payload: packPayload,
			})
		cdrBlocks.PackagesQty += 1
	}
	return nil
}

func newCirpackCdrBlocks() *CirpackCdrBlocks {
	return &CirpackCdrBlocks{}
}

func (cdrBlocks *CirpackCdrBlocks) SetIngestTime() {
	cdrBlocks.IngestTime = global.GetBatchStartTime()
}

func (cdrBlocks *CirpackCdrBlocks) ReadBlocksFromStringCdr(cdrString string) error {
	cdrBlocks.SetIngestTime()
	remainingSting, err := cdrBlocks.ReadHeaderBlock(cdrString)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to read header block from CDR")
		global.Logger.WithFields(logrus.Fields{
			"data": cdrString,
		}).Debug("CDR data")
		return err
	}
	err = cdrBlocks.ReadPackagesBlocks(remainingSting)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to read optional packages blocks from CDR")
		global.Logger.WithFields(logrus.Fields{
			"data":      cdrString,
			"remaining": remainingSting,
		}).Debug("CDR data")
		return err
	}
	return nil
}

func (cdrBlocks *CirpackCdrBlocks) deserializeCirpackCdrBlocks(cdrString string) *CirpackCdrBlocks {
	err := cdrBlocks.ReadBlocksFromStringCdr(cdrString)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to read blocks from CDR")
		cdrBlocks = nil // garbage
		return nil
	}
	return cdrBlocks
}

// end of CirpackCdrBlocks methods

// tools to deserialize cirpack CDR blocks payload into cirpack CDR struct fields

func loadCdrPayloadToPackageStruct(pack interface{}, payload string) error {
	fields := strings.Split(payload, " ")
	if len(fields) <= 1 {
		err := fmt.Errorf("unable to load an empty payload")
		return err
	}
	// let's check that the struct can receive
	// payload fields
	s := reflect.ValueOf(pack).Elem()
	if len(fields) != s.NumField() {
		err := fmt.Errorf("struct and payload don't have the same amount of fields")
		return err
	}
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		switch f.Type().String() {
		case "string":
			{
				f.SetString(fields[i])
			}
		case "int", "int64":
			{
				if myInt, err := strconv.ParseInt(fields[i], 10, 64); err == nil {
					f.SetInt(myInt)
				}
			}
		case "uint":
			{
				if myUInt, err := strconv.ParseUint(fields[i], 10, 64); err == nil {
					f.SetUint(myUInt)
				}
			}
		default:
			{
				global.Logger.Warn("unmanaged type during load of payload")
			}
		}
	}
	return nil
}

// tool to copy all fields from a source structure to a destination structure sharing the same json tag

func CopySameTaggedField(source interface{}, destination interface{}) error {
	src := reflect.ValueOf(source).Elem()
	if !src.CanAddr() {
		return fmt.Errorf("cannot assign to source, source must be a pointer in order to assign")
	}
	dst := reflect.ValueOf(destination).Elem()
	if !dst.CanAddr() {
		return fmt.Errorf("cannot assign to destination, destination must be a pointer in order to assign")
	}
	// It's possible we can cache this, which is why precompute all these ahead of time.
	findJsonName := func(t reflect.StructTag) (string, error) {
		if jt, ok := t.Lookup("json"); ok {
			return strings.Split(jt, ",")[0], nil
		}
		return "", fmt.Errorf("tag provided does not define a json tag")
	}

	destinationFieldNames := map[string]int{}
	for i := 0; i < dst.NumField(); i++ {
		typeField := dst.Type().Field(i)
		tag := typeField.Tag
		jname, _ := findJsonName(tag)
		destinationFieldNames[jname] = i
	}

	//sourceFieldNames := map[string]int{}
	for i := 0; i < src.NumField(); i++ {
		typeField := src.Type().Field(i)
		tag := typeField.Tag
		jname, _ := findJsonName(tag)
		fieldDstNum, ok := destinationFieldNames[jname]
		if !ok {
			global.Logger.WithField("json tag", jname).Debug("json tag does not exit in dst struct")
		} else {
			destField := dst.Field(fieldDstNum)
			srcField := src.Field(i)
			destField.Set(srcField)
		}
	}
	return nil
}

// small function to convert and ip from its hexadecimal format to dotted format

func convertHexaIpToStringIp(myHexaIp string) string {
	var ipAddress [4]int64
	var indexIp int
	for index := 0; index < 8; index += 2 {
		indexIp = index / 2
		myByte, _ := strconv.ParseInt(myHexaIp[index:index+2], 16, 32)
		ipAddress[indexIp] = myByte
	}
	return fmt.Sprintf("%d.%d.%d.%d", ipAddress[0], ipAddress[1], ipAddress[2], ipAddress[3])
}

// Methods of Cirpack packages (must comply to CirpackPackage interface)
// Basic package

func (basicPack *CirpackBasicPackage) LoadCdrFieldsFromBlockPayload(payload string) error {
	err := loadCdrPayloadToPackageStruct(basicPack, payload)
	if err != nil {
		return err
	}
	return nil
}

func (basicPack *CirpackBasicPackage) FulfilCdrTsJsonWithCustomFields(cdrTsModel *CirpackCdrJsonTimeSeries) error {
	// let's deal with 	CallStartDate, CallStartTime
	dateTimeStr := basicPack.CallStartDate[0:4] + "-" + basicPack.CallStartDate[4:6] +
		"-" + basicPack.CallStartDate[6:8]
	dateTimeStr = dateTimeStr + "T" + basicPack.CallStartTime[0:2] + ":" + basicPack.CallStartTime[2:4] +
		":" + basicPack.CallStartTime[4:6] + "Z"
	t, err := time.Parse(time.RFC3339, dateTimeStr)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to parse cdr date and time")
		return err
	}
	cdrTsModel.CallStartDateTime = t.Unix()
	// let's convert ip address of the switch to a readable value
	cdrTsModel.SwitchIp = convertHexaIpToStringIp(cdrTsModel.SwitchIp)
	return nil
}

func (basicPack *CirpackBasicPackage) FulfilCdrTsJson(cdrTsModel *CirpackCdrJsonTimeSeries) error {
	err := CopySameTaggedField(basicPack, cdrTsModel)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to copy basic package to ts model")
	}
	err = basicPack.FulfilCdrTsJsonWithCustomFields(cdrTsModel)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to copy basic package custom fields to ts model")
	}
	return nil
}



// RTP package

func (rtpPack *CirpackRtpPackage) LoadCdrFieldsFromBlockPayload(payload string) error {
	err := loadCdrPayloadToPackageStruct(rtpPack, payload)
	if err != nil {
		return err
	}
	return nil
}


func (rtpPack *CirpackRtpPackage) FulfilCdrTsJsonWithCustomFields(cdrTsModel *CirpackCdrJsonTimeSeries) error {
	// let's deal with ip and port field
	fields := strings.Split(rtpPack.RtpIpPort, ":")
	if len(fields) != 2 {
		global.Logger.Warn("error in rtp package ip and port field")
	}
	cdrTsModel.RtpIp = fields[0]
	cdrTsModel.RtpPort = fields[1]
	return nil
}

func (rtpPack *CirpackRtpPackage) FulfilCdrTsJson(cdrTsModel *CirpackCdrJsonTimeSeries) error {
	err := CopySameTaggedField(rtpPack, cdrTsModel)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to copy basic package")
	}
	err = rtpPack.FulfilCdrTsJsonWithCustomFields(cdrTsModel)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to copy basic package custom fields to ts model")
	}
	return nil
}



// Undefined package

func (rtpPack *CirpackUndefinedPackage) FulfilCdrTsJson(cdrTsModel *CirpackCdrJsonTimeSeries) error {
	return fmt.Errorf("cannot fulfil TS with undefined package")
}

func (rtpPack *CirpackUndefinedPackage) LoadCdrFieldsFromBlockPayload(payload string) error {
	return fmt.Errorf("undefined block")
}

// Methods of CirpackCdr struct

func NewCirpackCdr() *CirpackCdr {
	return &CirpackCdr{}
}

func (cdr *CirpackCdr) loadCdrIngestTimeFieldsFromBlocks(cdrBlocks *CirpackCdrBlocks) error {
	cdr.IngestTime = cdrBlocks.IngestTime
	return nil
}

func (cdr *CirpackCdr) loadCdrHeaderFieldsFromBlocks(cdrBlocks *CirpackCdrBlocks) error {
	headerSlice := splitCdrHeader(cdrBlocks.Header)
	cdr.Header.AppFlag = headerSlice[0]
	cdr.Header.Length = cdrBlocks.Length
	return nil
}

func (cdr *CirpackCdr) loadCdrPackagesFromBlocks(cdrBlocks *CirpackCdrBlocks) error {
	for _, packBlock := range cdrBlocks.Packages {
		switch packBlock.Type {
		case "T":
			{
				newPack := new(CirpackBasicPackage)
				err := newPack.LoadCdrFieldsFromBlockPayload(packBlock.Payload)
				if err != nil {
					global.Logger.WithError(err).Warn("issue while loading cdr fields from std block")
					return err
				}
				cdr.Packages = append(cdr.Packages, newPack)
			}
		case "O":
			{
				newPack := new(CirpackRtpPackage)
				err := newPack.LoadCdrFieldsFromBlockPayload(packBlock.Payload)
				if err != nil {
					global.Logger.WithError(err).Warn("issue while loading cdr fields from rtp block")
					return err
				}
				cdr.Packages = append(cdr.Packages, newPack)
			}
		default:
			{
				global.Logger.Debug("skipping undefined package")
			}
		}
	}
	return nil
}

func (cdr *CirpackCdr) loadCdrFromBlocks(cdrBlocks *CirpackCdrBlocks) error {
	err := cdr.loadCdrIngestTimeFieldsFromBlocks(cdrBlocks)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to load cdr ingest time from block ingest time")
		return err
	}
	err = cdr.loadCdrHeaderFieldsFromBlocks(cdrBlocks)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to load cdr header from block header")
		return err
	}
	err = cdr.loadCdrPackagesFromBlocks(cdrBlocks)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to load cdr fields from package blocks")
		return err
	}
	return nil
}

func (cdr *CirpackCdr) loadCdrFieldsFromCdrPayload(payload string) error {
	cdrBlocks := newCirpackCdrBlocks()
	cdrBlocks.deserializeCirpackCdrBlocks(payload)
	if cdrBlocks == nil {
		err := fmt.Errorf("unable to deserialize cirpack cdr blocks")
		return err
	}
	err := cdr.loadCdrFromBlocks(cdrBlocks)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to load blocks to cdr")
		return err
	}
	return nil
}

// exported functions used by other packages

// function taking a CDR input as a string of raw data, deserialize this raw data as one CDR,
// convert it into time series CDR, marshal this TS struct into JSON and return it as a string
// it also returns potential errors during processing

func ConvertCirpackCdrFromRawStringToJsonString(cdrData string) (string, error) {
	var result string
	cirpackCdr := NewCirpackCdr()
	err := cirpackCdr.loadCdrFieldsFromCdrPayload(cdrData)
	if err != nil {
		global.Logger.Warn("unable load cdr fields from raw cdr data")
		return "", err
	}
	cdrTs := NewCirpackCdrJsonTimeSeries()
	err = cdrTs.LoadData(cirpackCdr)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to load time series cdr model")
	} else {
		err := cdrTs.EnrichData()
		if err != nil {
			global.Logger.Warn("unable to enrich cdr")
		}
		jsonOutput, err := json.Marshal(cdrTs)
		if err != nil {
			global.Logger.Warn("unable to marshal cdr time series model")
		} else {
			result = string(jsonOutput) + "\n"
		}
	}
	return result, nil
}


func ConvertCirpackCdrsFromFileToJsonStringSlice(cdrFile string) ([]string, error) {
	var returnedSlice []string
	if cdrFileDesc, err := os.Open(cdrFile); err == nil {
		defer cdrFileDesc.Close()
		// create a new scanner and read the file line by line
		scanner := bufio.NewScanner(cdrFileDesc)
		for scanner.Scan() {
			cdrJsonString, err := ConvertCirpackCdrFromRawStringToJsonString(scanner.Text())
			if err != nil {
				global.Logger.WithError(err).Warn("unable to convert it JSON a raw cdr")
			} else {
				returnedSlice = append(returnedSlice, cdrJsonString)
			}
		}
		if err = scanner.Err(); err != nil {
			global.Logger.WithError(err).Fatal("unable to scan cdr files line by line")
		}

	} else {
		global.Logger.WithError(err).Fatal("Unable to open cdr file")
	}
	return returnedSlice, nil
}

// function opening a file of CDR's, deserialize all CDR, convert it into time series CDR
// and write the result into a file descriptor. Is used for batch mode of main program
// Returns amount of bytes written and potential error during processing

func ConvertCirpackCdrsToJsonFromFileToFileDescriptor(cdrFile string, myOutPutFile *os.File) (int, error) {
	var writtenBytes int = 0
	var writeErr error
	returnedSlice, err := ConvertCirpackCdrsFromFileToJsonStringSlice(cdrFile)
	if err != nil {
		global.Logger.WithError(err).Warn("unable to convert cdr file to slice")
	}
	for _, cdrJsonStr := range returnedSlice {
		writtenBytes, writeErr = myOutPutFile.WriteString(cdrJsonStr)
		if writeErr != nil {
			global.Logger.WithError(err).Warn("io error with file descriptor")
			return 0, writeErr
		}
	}
	return writtenBytes, nil
}


// This function is reading the content of a CDR folder (produce by cirpack cdr tool)
// Read CDR files and return a slice of strings (JSON CDR time series model)
func ConvertCdrFolderToCdrs(myCdrDirInput string, purgeCdrFiles bool) ([]string, error) {
	var returnCdrs []string

	myCdrFiles, err := global.ReadCdrFolder(myCdrDirInput)
	if err != nil {
		global.Logger.WithError(err).Fatal("unable to read cdr folder")
	}
	if len(myCdrFiles) == 0 {
		global.Logger.Info("CDR folder is empty waiting for next batch cycle")
		return nil, nil
	}
	for _, cdrFile := range myCdrFiles {
		cdrsSlice, err := ConvertCirpackCdrsFromFileToJsonStringSlice(cdrFile)
		if err != nil {
			global.Logger.WithError(err).Warn("unable to convert cdr file into slice of JSON CDR")
		}
		returnCdrs = append(returnCdrs, cdrsSlice...)
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
