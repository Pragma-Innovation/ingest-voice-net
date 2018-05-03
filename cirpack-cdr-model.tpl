{
  "type" : "index_hadoop",
  "spec" : {
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "paths" : "{{ batchContentFile }}"
      }
    },
    "dataSchema" : {
      "dataSource" : "pstn_cdr",
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "DAY",
        "queryGranularity" : "NONE",
        "intervals" : ["{{ batchIntervals }}"]
      },
      "parser" : {
        "type" : "hadoopyString",
        "parseSpec" : {
          "format" : "json",
          "dimensionsSpec" : {
            "dimensions" : [
              "account",
              "direction",
              "time",
              "switch_ip",
              "access_code",
              "type_calling_party_access",
              "numbering_plan_calling_party_number",
              "calling_party_category",
              "nature_calling_number",
              "calling_number",
              "nature_additional_calling_party_address",
              "additional_calling_party_address",
              "access_type_called_number",
              "number_plan_called_party",
              "nature_called_number",
              "called_num",
              "category_real_called_number",
              "type_real_called_number",
              "nat_real_called",
              "real_called_number",
              "billing_mode",
              "service_code",
              "release_loc_cause",
              "operator_id",
              "circuit_id",
              "in_trunk_group",
              "out_trunk_group"
            ]
          },
          "timestampSpec" : {
            "format" : "auto",
            "column" : "time"
          }
        }
      },
      "metricsSpec" : [
        {
          "name" : "count",
          "type" : "count"
        },
        {
          "name" : "connect_duration",
          "type" : "longSum",
          "fieldName" : "connect_duration"
        },
        {
          "name" : "connnection_ringing_duration",
          "type" : "longSum",
          "fieldName" : "connnection_ringing_duration"
        },
        {
          "name" : "total_duration",
          "type" : "longSum",
          "fieldName" : "total_duration"
        },
        {
          "name" : "max_duration",
          "type" : "longMax",
          "fieldName" : "total_duration"
        },
        {
          "name" : "min_duration",
          "type" : "longMin",
          "fieldName" : "total_duration"
        }
      ]
    },
    "tuningConfig" : {
      "type" : "hadoop",
      "partitionsSpec" : {
        "type" : "hashed",
        "targetPartitionSize" : 5000000
      },
      "jobProperties" : {}
    }
  }
}
