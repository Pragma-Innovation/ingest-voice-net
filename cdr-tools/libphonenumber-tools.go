package cdrtools

import (
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/nyaruka/phonenumbers"
	"github.com/sirupsen/logrus"
	"strconv"
)

var descNumTypeFromType = map[phonenumbers.PhoneNumberType]string {
	phonenumbers.FIXED_LINE:           "fixed line",
	phonenumbers.MOBILE:               "mobile",
	phonenumbers.FIXED_LINE_OR_MOBILE: "fixed line or mobile",
	phonenumbers.TOLL_FREE:            "toll free",
	phonenumbers.PREMIUM_RATE:         "premium rate",
	phonenumbers.SHARED_COST:          "shared cost",
	phonenumbers.VOIP:                 "voip",
	phonenumbers.PERSONAL_NUMBER:      "personal number",
	phonenumbers.PAGER:                "pager",
	phonenumbers.UAN:                  "uan",
	phonenumbers.VOICEMAIL:            "voicemail",
	phonenumbers.UNKNOWN:              "unknown",
}

func PstnColumnFromCalledNum(myNumber string) string {
	num, err := phonenumbers.Parse(myNumber, "FR")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error": err,
			"number": myNumber,
		}).Info("unable to parse number")
		return ""
	}

	geo, err := phonenumbers.GetGeocodingForNumber(num, "fr")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error": err,
			"number": myNumber,
		}).Info("unable to GeoCode this number")
	}
	result := "\"called_country_code\": \"" + strconv.Itoa(int(num.GetCountryCode())) + "\", \"called_country\": \"" +
		phonenumbers.GetRegionCodeForNumber(num) + "\", \"called_number_type\": \"" +
		descNumTypeFromType[phonenumbers.GetNumberType(num)] + "\", \"called_number_location\": \"" +
		geo + "\""
	return result
}

func PstnColumnFromCallingNum(myNumber string) string {
	num, err := phonenumbers.Parse(myNumber, "FR")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error": err,
			"number": myNumber,
		}).Info("unable to parse number")
		return ""
	}

	geo, err := phonenumbers.GetGeocodingForNumber(num, "fr")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error": err,
			"number": myNumber,
		}).Info("unable to GeoCode this number")
	}
	result := "\"calling_country_code\": \"" + strconv.Itoa(int(num.GetCountryCode())) + "\", \"calling_country\": \"" +
		phonenumbers.GetRegionCodeForNumber(num) + "\", \"calling_number_type\": \"" +
		descNumTypeFromType[phonenumbers.GetNumberType(num)] + "\", \"calling_number_location\": \"" +
		geo + "\""
	return result
}