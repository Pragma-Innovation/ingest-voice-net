# ingest-voice-net

# Purpose of this tool

Tool to ingest Cirpack voice CDR on one end and produce it to Kafka on the other end

Once deserialized CDR data can be stored in a time series database for use cases like:
* Engineering,
* Troubleshooting,
* Charging,
* Customer reporting,
* Fraud detection,
* ...

# Main features

* Cirpack CDR deserializer: convert cirpack data structure into Go structure that can be marshall or unmarshall into JSON data
* Use libphonenumber library to enrich data with called number and calling number geoloc, number types (fixe, mobile, toll free, ...)
* Kafka producer,

# Configuration

The initial goal of this tool was to connect to cirpack switches via Tcp sockets receiving  CDR's the same way as it does with the carrier BSS. Unfortunately, the way Cirpack switches establish TCP sockets and exchange CDR's is not open for open source development. To work around this issue, this tool have to be used in micro batch jointly with a Cirpack tool that dump CDR's into files. 

Systemd unit snippet of Cirpack tool configuration:

```
ExecStart=/usr/local/bin/ipc2netsrv64 -compat32 0x1234000a 2090
```

Systemd unit snippet of the tool:

```
ExecStart=/usr/local/bin/ingest-voice-net \
-mode batch-stream -path-cdr /home/omni/tickets -kafka-ip 10.5.13.130 -kafka-port 9092 \
-kafka-topic voipcdr -batch-loop 120s -log warning\
```


