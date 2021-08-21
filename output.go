package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
)

type Output struct {
	Concurrent      int
	Database        string
	DSN             string
	DataChan        chan []byte
	Count           int64
	TotalCount      int64
	WriteToTDengine bool
	Batch           int
}

func (o *Output) Start(ch chan []byte, url string) {
	for i := 0; i < o.Concurrent; i++ {
		w, err := NewWorker(o.DSN, o.Database, o.WriteToTDengine, o.Batch)
		if err != nil {
			log.Fatal(err)
		}

		go w.Start(o.DataChan)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				count := atomic.LoadInt64(&o.Count)
				atomic.StoreInt64(&o.Count, 0)
				log.Printf("receive %d in last second, qos is %.2f, totalCount is %d", count, float64(count)/10.0, atomic.LoadInt64(&o.TotalCount))
			case msg := <-ch:
				// 将消息转发到其他worker
				atomic.AddInt64(&o.Count, 1)
				atomic.AddInt64(&o.TotalCount, 1)
				o.DataChan <- msg
			}
		}
	}()
}

type Worker struct {
	db *sql.DB

	createSQLBuf *bytes.Buffer
	dataBuf      *bytes.Buffer
	schemaBuf    *bytes.Buffer

	Database string
	WriteTo  bool
	Batch    int

	CreateTableMap map[string]bool

	DataSQLs []string
	count    int
}

func NewWorker(dsn, database string, writeTo bool, batch int) (*Worker, error) {
	w := &Worker{
		createSQLBuf: &bytes.Buffer{},
		schemaBuf:    &bytes.Buffer{},
		dataBuf:      &bytes.Buffer{},

		Database: database,
		WriteTo:  writeTo,
		Batch:    batch,

		CreateTableMap: map[string]bool{},
	}

	taos, err := sql.Open("taosSql", url)
	if err != nil {
		fmt.Println("failed to connect TDengine, err:", err)
		return nil, err
	}

	sqlStr := fmt.Sprintf("create database if not exists %s precision 'ns'", w.Database)
	if _, err := taos.Exec(sqlStr); err != nil {
		return nil, err
	}

	if _, err := taos.Exec("use " + w.Database); err != nil {
		return nil, err
	}

	w.db = taos

	return w, nil
}

func (w *Worker) Start(ch chan []byte) {
	for msg := range ch {
		if err := w.Write(msg); err != nil {
			log.Println(err)
		}
	}
}

func (w *Worker) Write(data []byte) error {
	// this buffer is used to format values
	dataBuf := w.dataBuf
	// this buffer is used  to create tables
	createTableBuffer := w.createSQLBuf
	// this buffer is used to format schemas
	schemaBuf := w.schemaBuf
	// the final buffer

	dataBuf.Reset()
	createTableBuffer.Reset()
	schemaBuf.Reset()

	currentTimestamp := time.Now().UnixNano()
	deviceIDBytes, _, _, err := jsonparser.Get(data, "d_id")
	if err != nil {
		return err
	}

	deviceID := string(deviceIDBytes)
	deviceID = strings.ReplaceAll(deviceID, "-", "_")

	if !w.CreateTableMap[deviceID] {
		createTableBuffer.WriteString("CREATE TABLE IF NOT EXISTS ")
		createTableBuffer.WriteByte('"')
		createTableBuffer.WriteString(strings.ReplaceAll(deviceID, "-", "_"))
		createTableBuffer.WriteByte('"')
		createTableBuffer.WriteString(` ("time" TIMESTAMP,"reporttime" TIMESTAMP,"reporttimestamp" BIGINT`)
	}

	schemaBuf.WriteString(`"`)
	schemaBuf.WriteString(w.Database)
	schemaBuf.WriteString(`"."`)
	schemaBuf.WriteString(strings.ReplaceAll(deviceID, "-", "_"))
	schemaBuf.WriteString(`" (`)
	schemaBuf.WriteString("time,reporttime,reporttimestamp")

	ts, _, _, err := jsonparser.Get(data, "ts")
	if err != nil {
		return err
	}

	parsedReportTimestamp, err := ParseTime(string(ts))
	if err != nil {
		return err
	}

	dataBuf.WriteString("(")
	dataBuf.WriteString(strconv.FormatInt(currentTimestamp, 10))
	dataBuf.WriteString(",")
	dataBuf.WriteString(strconv.FormatInt(parsedReportTimestamp, 10))
	dataBuf.WriteString(",")
	dataBuf.WriteString(strconv.FormatInt(parsedReportTimestamp, 10))

	currentTimestamp++

	if err := jsonparser.ObjectEach([]byte(data), func(deviceServiceKey []byte, deviceServiceValue []byte, dataType jsonparser.ValueType, offset int) error {
		if err := jsonparser.ObjectEach(deviceServiceValue, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			val, valueDataType, _, err := jsonparser.Get(value, "value")
			if err != nil {
				return err
			}

			if !w.CreateTableMap[deviceID] {
				createTableBuffer.WriteString(`,"`)
				createTableBuffer.Write(deviceServiceKey)
				createTableBuffer.WriteString("_")
				createTableBuffer.WriteString(strings.ReplaceAll(string(key), "-", "_"))
				createTableBuffer.WriteByte('"')
			}

			schemaBuf.WriteString(`,"`)
			schemaBuf.Write(deviceServiceKey)
			schemaBuf.WriteString("_")
			schemaBuf.WriteString(strings.ReplaceAll(string(key), "-", "_"))
			schemaBuf.WriteByte('"')

			dataBuf.WriteString(",")
			switch valueDataType {
			case jsonparser.Boolean:
				if !w.CreateTableMap[deviceID] {
					createTableBuffer.WriteString(" BOOL")
				}

				dataBuf.Write(val)
			case jsonparser.Number:
				if !w.CreateTableMap[deviceID] {
					createTableBuffer.WriteString(" DOUBLE")
				}

				dataBuf.Write(val)
			case jsonparser.String:
				if !w.CreateTableMap[deviceID] {
					createTableBuffer.WriteString(" NCHAR(100)")
				}

				dataBuf.WriteString("'")
				dataBuf.Write(val)
				dataBuf.WriteString("'")
			default:
				return fmt.Errorf("unsupport ")
			}

			return nil
		}, "properties"); err != nil {
			return err
		}

		return nil
	}, "services"); err != nil {
		return err
	}

	if !w.CreateTableMap[deviceID] {
		createTableBuffer.WriteString(")")
		if _, err := w.db.Exec(createTableBuffer.String()); err != nil {
			return err
		}

		w.CreateTableMap[deviceID] = true
	}

	schemaBuf.WriteString(") values ")
	dataBuf.WriteString(")")
	schemaBuf.Write(dataBuf.Bytes())

	w.DataSQLs = append(w.DataSQLs, schemaBuf.String())
	w.count++
	if w.count >= w.Batch {
		dataBuf.Reset()

		dataBuf.WriteString("insert into ")
		for i := 0; i < len(w.DataSQLs); i++ {
			dataBuf.WriteString(" ")
			dataBuf.WriteString(w.DataSQLs[i])
		}
		w.count = 0

		w.DataSQLs = w.DataSQLs[:0]

		if w.WriteTo {
			if _, err := w.db.Exec(dataBuf.String()); err != nil {
				log.Println(err)
			}
		}
	}

	return nil
}
