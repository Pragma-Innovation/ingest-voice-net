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

// Function that bundle lobphonenumber call. Takes a phone number as input
// returns 4 strings : country code, country, location, phone number type

func LibPhoneNumberEnrichFields(myNumber string) (string, string, string, string) {
	var num *phonenumbers.PhoneNumber
	var err error
	var geo string
	num, err = phonenumbers.Parse(myNumber, "FR")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error": err,
			"number": myNumber,
		}).Info("unable to parse number")
		return "", "", "", ""
	}

	geo, err = phonenumbers.GetGeocodingForNumber(num, "fr")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error": err,
			"number": myNumber,
		}).Info("unable to GeoCode this number")
		return "", "", "", ""
	}
	cc := strconv.Itoa(int(num.GetCountryCode()))
	country := phonenumbers.GetRegionCodeForNumber(num)
	numType := descNumTypeFromType[phonenumbers.GetNumberType(num)]
	return cc, country, geo, numType
}
