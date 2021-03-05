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
	"flag"
	cdrtools "github.com/Pragma-Innovation/ingest-voice-net/cdr-tools"
	"github.com/Pragma-Innovation/ingest-voice-net/global"
	"golang.org/x/exp/errors/fmt"
)

var (
	number = flag.String("num", "", "MANDATORY: e164 format of phone number to be tested")
)

func main() {
	flag.Parse()
	if flag.NFlag() != 1 || number == nil {
		global.Logger.Warn("must pass phone number to maim program")
	}
	cc, country, place, phoneType := cdrtools.LibPhoneNumberEnrichFields(*number)
	fmt.Println(cc, country, place, phoneType)
}
