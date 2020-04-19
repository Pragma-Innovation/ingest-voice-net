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
    cc, country, place, phoneType :=cdrtools.LibPhoneNumberEnrichFields(*number)
    fmt.Println(cc, country, place, phoneType)
}
