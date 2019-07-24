package cdrtools

import "time"

// CDR model for flat JSON struct

type CirCdrJsonFlat struct {
	SgtTimeStamp                        time.Time `json:"__time"`
	SgtCount                            uint      `json:"count"`
	AppFlag                             string    `json:"app_flag"`
	IngestTime                          string    `json:"ingest_time"`
	Account                             string    `json:"account"`
	Direction                           string    `json:"direction"`
	CallStartDate                       string    `json:"call_start_date"`
	CallStartHour                       string    `json:"call_start_hour"`
	CallStartMinsec                     string    `json:"call_start_minsec"`
	ConnectionDuration                  uint      `json:"connection_duration"`
	ConnectionRinginDuration            uint      `json:"connnection_ringing_duration"`
	TotalDuration                       uint      `json:"total_duration"`
	SwitchIp                            string    `json:"switch_ip"`
	AccessCode                          string    `json:"access_code"`
	TypeCallingPartyAccess              string    `json:"type_calling_party_access"`
	NumberPlanCallingPartyNumber        string    `json:"numbering_plan_calling_party_number"`
	CallingPartyCategory                string    `json:"calling_party_category"`
	NatureCallingNumber                 string    `json:"nature_calling_number"`
	CallingNumber                       string    `json:"calling_number"`
	NatureAdditionalCallingPartyAddress string    `json:"nature_additional_calling_party_address"`
	AdditionalCallingPartyAddress       string    `json:"additional_calling_party_address"`
	AccessTypeCalledNumber              string    `json:"access_type_called_number"`
	NumberPlanCalledParty               string    `json:"number_plan_called_party"`
	NatureCalledNumber                  string    `json:"nature_called_number"`
	CalledNum                           string    `json:"called_num"`
	CategoryRealCalledNumber            string    `json:"category_real_called_number"`
	TypeRealCalledNumber                string    `json:"type_real_called_number"`
	NatRealCalled                       string    `json:"nat_real_called"`
	RealCalledNumber                    string    `json:"real_called_number"`
	BillingMode                         string    `json:"billing_mode"`
	ServceCode                          string    `json:"service_code"`
	ReleaseLoccause                     string    `json:"release_loc_cause"`
	OperatorId                          string    `json:"operator_id"`
	CircuitId                           string    `json:"circuit_id"`
	InTrunkGroup                        string    `json:"in_trunk_group"`
	OutTrunkGroup                       string    `json:"out_trunk_group"`
	RtpDuration                         uint      `json:"rtp_duration"`
	RtpBytesSent                        uint      `json:"rtp_bytes_sent"`
	RtpBytesReceived                    uint      `json:"rtp_bytes_received"`
	RtpPacketSent                       uint      `json:"rtp_pck_sent"`
	RtpPacketreceived                   uint      `json:"rtp_pck_received"`
	RtpPacketLost                       uint      `json:"rtp_pck_lost"`
	RtpAvgJitter                        uint      `json:"rtp_avg_jitter"`
	RtpAvgTransDelay                    uint      `json:"rtp_avg_trans_delay"`
	RtpAddiInfo                         string    `json:"rtp_addi_info"`
	RtpIp                               string    `json:"rtp_ip"`
	RtpPort                             string    `json:"rtp_port"`
	CalledCountryCode                   string    `json:"called_country_code"`
	CalledCountry                       string    `json:"called_country"`
	CalledNumberType                    string    `json:"called_number_type"`
	CalledNumberLocation                string    `json:"called_number_location"`
}