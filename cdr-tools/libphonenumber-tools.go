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

package cdrtools

import (
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"github.com/nyaruka/phonenumbers"
	"github.com/sirupsen/logrus"
	"strconv"
)

var descNumTypeFromType = map[phonenumbers.PhoneNumberType]string{
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
			"error":  err,
			"number": myNumber,
		}).Info("unable to parse number")
		return "", "", "", ""
	}

	geo, err = phonenumbers.GetGeocodingForNumber(num, "fr")
	if err != nil {
		global.Logger.WithFields(logrus.Fields{
			"error":  err,
			"number": myNumber,
		}).Info("unable to GeoCode this number")
		return "", "", "", ""
	}
	cc := strconv.Itoa(int(num.GetCountryCode()))
	country := phonenumbers.GetRegionCodeForNumber(num)
	numType := descNumTypeFromType[phonenumbers.GetNumberType(num)]
	return cc, country, geo, numType
}
