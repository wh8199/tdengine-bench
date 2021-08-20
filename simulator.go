package main

import (
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func FormatTime() string {
	return time.Now().Format("2006-01-02T15:04:05+08:00")
}

func ParseTime(timeStr string) (int64, error) {
	t, err := time.Parse("2006-01-02T15:04:05+08:00", timeStr)
	if err != nil {
		return 0, err
	}

	return t.UnixNano(), nil
}

type datas [][]byte

type Simulator struct {
	ReportInterval string `json:"reportInterval"`
	Concurrent     int    `json:"concurrent"`
	ReportCount    int    `json:"reportCount"`
	PropertyCount  int    `json:"propertyCount"`
	Prefix         string `json:"prefix"`
}

type DeviceProperty struct {
	Value    interface{} `json:"value"`
	DataType string      `json:"dataType"`
	ItemType string      `json:"itemType"`
}

type ReportService struct {
	DeviceProperties map[string]*DeviceProperty `json:"properties"`
}

type ReportMessage struct {
	ProductID      string                    `json:"p_id"`
	DeviceID       string                    `json:"d_id"`
	DeviceServices map[string]*ReportService `json:"services"`
	ReportTime     string                    `json:"ts"`
}

func (r *ReportMessage) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (s *Simulator) Start(ch chan []byte) {
	reportIntervalDuration := time.Second
	if temp, err := time.ParseDuration(s.ReportInterval); err == nil {
		reportIntervalDuration = temp
	}

	propertyCount := 100
	if s.PropertyCount > 0 {
		propertyCount = s.PropertyCount
	}

	prefix := s.Prefix
	if prefix == "" {
		prefix = "device"
	}

	for i := 0; i < s.Concurrent; i++ {
		go func(i int) {
			reportMessage := &ReportMessage{
				ProductID: "productID",
				DeviceID:  prefix + "-" + strconv.Itoa(i+1),
				DeviceServices: map[string]*ReportService{
					"simulator": {
						DeviceProperties: map[string]*DeviceProperty{},
					},
				},
			}

			for i := 1; i < propertyCount-1; i++ {
				reportMessage.DeviceServices["simulator"].DeviceProperties["property-"+strconv.Itoa(i)] = &DeviceProperty{
					DataType: "int",
					Value:    i,
				}
			}
			
			reportMessage.ReportTime = FormatTime()
			ret, _ := reportMessage.Marshal()

			ticker := time.NewTicker(reportIntervalDuration)
			defer ticker.Stop()
			neverStop := s.ReportCount == 0
			stopCounter := 0

			for range ticker.C {
				if !neverStop {
					if stopCounter >= s.ReportCount {
						return
					}
					stopCounter++
				}

				ch <- ret
			}
		}(i)
	}
}
