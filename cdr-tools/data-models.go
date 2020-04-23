package cdrtools

import (
    "fmt"
    "github.com/Pragma-Innovation/ingest-voice-net/global"
    "strings"
)

// CDR model for flat JSON struct

type CirpackCdrJsonTimeSeries struct {
    AppFlag                             string `json:"app_flag"`
    IngestTime                          int64  `json:"ingest_time"`
    Account                             string `json:"account"`
    Direction                           string `json:"direction"`
    CallStartDateTime                   int64  `json:"call_start_date_time"`
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
    RtpDuration                         uint   `json:"rtp_duration"`
    RtpBytesSent                        uint   `json:"rtp_bytes_sent"`
    RtpBytesReceived                    uint   `json:"rtp_bytes_received"`
    RtpPacketSent                       uint   `json:"rtp_pck_sent"`
    RtpPacketReceived                   uint   `json:"rtp_pck_received"`
    RtpPacketLost                       uint   `json:"rtp_pck_lost"`
    RtpAvgJitter                        uint   `json:"rtp_avg_jitter"`
    RtpAvgTransDelay                    uint   `json:"rtp_avg_trans_delay"`
    RtpAddiInfo                         string `json:"rtp_addi_info"`
    RtpIp                               string `json:"rtp_ip"`
    RtpPort                             string `json:"rtp_port"`
    CalledCountryCode                   string `json:"called_country_code"`
    CalledCountry                       string `json:"called_country"`
    CalledNumberType                    string `json:"called_number_type"`
    CalledNumberLocation                string `json:"called_number_location"`
    CallingCountryCode                  string `json:"calling_country_code"`
    CallingCountry                      string `json:"calling_country"`
    CallingNumberType                   string `json:"calling_number_type"`
    CallingNumberLocation               string `json:"calling_number_location"`
}

// Methods of data model defined struct CirpackCdrJsonTimeSeries

func NewCirpackCdrJsonTimeSeries() *CirpackCdrJsonTimeSeries {
    return &CirpackCdrJsonTimeSeries{}
}

func (cdrTs *CirpackCdrJsonTimeSeries) LoadCdrHeader(cdr *CirpackCdr) {
    cdrTs.IngestTime = cdr.IngestTime
    cdrTs.AppFlag = cdr.Header.AppFlag
}

func (cdrTs *CirpackCdrJsonTimeSeries) LoadCdrPackages(cdr *CirpackCdr) error {
    for _, cdrPack := range cdr.Packages {
        err := cdrPack.FulfilCdrTsJson(cdrTs)
        if err != nil {
            global.Logger.WithError(err).Warn("unable to load package to cdr time series")
            return err
        }
    }
    return nil
}

func (cdrTs *CirpackCdrJsonTimeSeries) LoadData(cdr *CirpackCdr) error {
    cdrTs.LoadCdrHeader(cdr)
    err := cdrTs.LoadCdrPackages(cdr)
    if err != nil {
        global.Logger.WithError(err).Warn("failed to load data to TS cdr")
        return err
    }
    return nil
}

// Time series cdr model enrichment functions and methods

// Return a standard phone number considering its nature

func getStdNumFromRawNumber(myNatureOfNumber string, myNumber string) (string, error) {
    switch myNatureOfNumber {
    case "3": // national call
        return "+33" + myNumber, nil
    case "4": // international calls
        return "+" + myNumber, nil
    case "2":
        return "", fmt.Errorf("libphone cannot deal with unknown number")
    case "115":
        return "", fmt.Errorf("libphone cannot deal with short number")
    default:
        global.Logger.WithField("nature of real called", myNatureOfNumber).Warn("unknown nature")
        return "", fmt.Errorf("nature of real called unknown")
    }
}

func patchCcCountryIfFrOverseas(myNatureOfNumber string, myNumber string) (bool, string, string) {
    if myNatureOfNumber == "3" { // national calls
        if strings.HasPrefix(myNumber, "262") ||
            strings.HasPrefix(myNumber, "692") ||
            strings.HasPrefix(myNumber, "693") {
            return true, "262", "RE"
        } else if strings.HasPrefix(myNumber, "590") ||
            strings.HasPrefix(myNumber, "690") {
            return true, "590", "GP"
        } else if strings.HasPrefix(myNumber, "594") ||
            strings.HasPrefix(myNumber, "694") {
            return true, "594", "GF"
        } else if strings.HasPrefix(myNumber, "596") ||
            strings.HasPrefix(myNumber, "696") {
            return true, "596", "MQ"
        }
    }
    return false, "", ""
}

func (cdrTs *CirpackCdrJsonTimeSeries) EnrichData() error {
    // Enrich data based on called number
    switch cdrTs.NatureCalledNumber {
    case "2", "0":
        {
            cdrTs.CalledCountryCode = "unknown code"
            cdrTs.CalledCountry = "ZZ"
            cdrTs.CalledNumberType = "unknown type"
            cdrTs.CalledNumberLocation = "unknown location"
        }
    case "115":
        {
            cdrTs.CalledCountryCode = "33"
            cdrTs.CalledCountry = "FR"
            cdrTs.CalledNumberType = "short num"
            cdrTs.CalledNumberLocation = "unknown location"
        }
    default:
        calledNumStd, err := getStdNumFromRawNumber(cdrTs.NatureCalledNumber, cdrTs.CalledNumber)
        if err != nil {
            global.Logger.WithError(err).Warn("unable to get std number from raw called number")
        }
        // removing too buggy called number before calling the libphonenumber
        if !strings.Contains(calledNumStd, "+0") {
            cdrTs.CalledCountryCode, cdrTs.CalledCountry, cdrTs.CalledNumberLocation,
                cdrTs.CalledNumberType = LibPhoneNumberEnrichFields(calledNumStd)
            isOverSeas, cc, country := patchCcCountryIfFrOverseas(cdrTs.NatureCalledNumber, cdrTs.CalledNumber)
            if isOverSeas {
                cdrTs.CalledCountryCode = cc
                cdrTs.CalledCountry = country
            }
        }
    }
    switch cdrTs.NatureCallingNumber {
    case "2", "0":
        {
            cdrTs.CallingCountryCode = "unknown code"
            cdrTs.CallingCountry = "ZZ"
            cdrTs.CallingNumberType = "unknown type"
            cdrTs.CallingNumberLocation = "unknown location"
        }
    case "115":
        {
            cdrTs.CallingCountryCode = "33"
            cdrTs.CallingCountry = "FR"
            cdrTs.CallingNumberType = "short num"
            cdrTs.CallingNumberLocation = "unknown location"
        }
    default:
        callingNumStd, err := getStdNumFromRawNumber(cdrTs.NatureCallingNumber, cdrTs.CallingNumber)
        if err != nil {
            global.Logger.WithError(err).Warn("unable to get std number from raw called number")
        }
        // removing too buggy called number before calling the libphonenumber
        if !strings.Contains(callingNumStd, "+0") {
            cdrTs.CallingCountryCode, cdrTs.CallingCountry, cdrTs.CallingNumberLocation,
                cdrTs.CallingNumberType = LibPhoneNumberEnrichFields(callingNumStd)
            isOverSeas, cc, country := patchCcCountryIfFrOverseas(cdrTs.NatureCallingNumber, cdrTs.CallingNumber)
            if isOverSeas {
                cdrTs.CallingCountryCode = cc
                cdrTs.CallingCountry = country
            }
        }
    }
    return nil
}
