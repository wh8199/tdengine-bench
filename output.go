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
	Concurrent int
	Database   string
	DSN        string
	DataChan   chan []byte
	Count      int64

	TotalCount int64

	WriteToTDengine bool
}

func (o *Output) Start(ch chan []byte, url string) {
	for i := 0; i < o.Concurrent; i++ {
		w, err := NewWorker(o.DSN, o.Database, o.WriteToTDengine)
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
	sqlBuf       *bytes.Buffer

	params []interface{}

	// this map records the result of create table, if a table is created, the value will be true
	PrepareSQLMap map[string]*sql.Stmt
	Database      string

	WriteTo bool
}

func NewWorker(dsn, database string, writeTo bool) (*Worker, error) {
	w := &Worker{
		createSQLBuf:  &bytes.Buffer{},
		schemaBuf:     &bytes.Buffer{},
		sqlBuf:        &bytes.Buffer{},
		dataBuf:       &bytes.Buffer{},
		params:        []interface{}{},
		PrepareSQLMap: map[string]*sql.Stmt{},
		Database:      database,
		WriteTo:       writeTo,
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
	sqlBuf := w.sqlBuf

	w.params = w.params[:0]

	dataBuf.Reset()
	createTableBuffer.Reset()
	schemaBuf.Reset()
	sqlBuf.Reset()

	log.Println(w.PrepareSQLMap)

	currentTimestamp := time.Now().UnixNano()

	deviceIDBytes, _, _, err := jsonparser.Get(data, "d_id")
	if err != nil {
		return err
	}

	deviceID := string(deviceIDBytes)
	deviceID = strings.ReplaceAll(deviceID, "-", "_")

	if w.PrepareSQLMap[deviceID] == nil {
		createTableBuffer.WriteString("CREATE TABLE IF NOT EXISTS ")

		sqlBuf.WriteString("insert into ")
		sqlBuf.WriteByte('"')
		sqlBuf.WriteString(w.Database)
		sqlBuf.WriteByte('"')
		sqlBuf.WriteByte('.')
		sqlBuf.WriteByte('"')
		sqlBuf.WriteString(strings.ReplaceAll(deviceID, "-", "_"))
		sqlBuf.WriteByte('"')

		schemaBuf.WriteString("(")
	}

	if w.PrepareSQLMap[deviceID] == nil {
		createTableBuffer.WriteByte('"')
		createTableBuffer.WriteString(strings.ReplaceAll(deviceID, "-", "_"))
		createTableBuffer.WriteByte('"')
	}

	ts, _, _, err := jsonparser.Get(data, "ts")
	if err != nil {
		return err
	}

	parsedReportTimestamp, err := ParseTime(string(ts))
	if err != nil {
		return err
	}

	if w.PrepareSQLMap[deviceID] == nil {
		createTableBuffer.WriteString(` ("time" TIMESTAMP`)
		createTableBuffer.WriteString(`,"reporttime" TIMESTAMP`)
		createTableBuffer.WriteString(`,"reporttimestamp" BIGINT`)

		schemaBuf.WriteString("time")
		schemaBuf.WriteString(",reporttime")
		schemaBuf.WriteString(",reporttimestamp")

		dataBuf.WriteString("?,?,?")
	}

	currentTimestamp++

	w.params = append(w.params, currentTimestamp, parsedReportTimestamp, parsedReportTimestamp)
	currentTimestamp++

	if err := jsonparser.ObjectEach([]byte(data), func(deviceServiceKey []byte, deviceServiceValue []byte, dataType jsonparser.ValueType, offset int) error {
		if err := jsonparser.ObjectEach(deviceServiceValue, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			val, valueDataType, _, err := jsonparser.Get(value, "value")
			if err != nil {
				return err
			}

			if w.PrepareSQLMap[deviceID] == nil {
				createTableBuffer.WriteString(`,"`)
				createTableBuffer.Write(deviceServiceKey)
				createTableBuffer.WriteString("_")
				createTableBuffer.WriteString(strings.ReplaceAll(string(key), "-", "_"))
				createTableBuffer.WriteByte('"')

				schemaBuf.WriteString(`,"`)
				schemaBuf.Write(deviceServiceKey)
				schemaBuf.WriteString("_")
				schemaBuf.WriteString(strings.ReplaceAll(string(key), "-", "_"))
				schemaBuf.WriteByte('"')

				dataBuf.WriteString(",?")
			}

			switch valueDataType {
			case jsonparser.Boolean:
				if w.PrepareSQLMap[deviceID] == nil {
					createTableBuffer.WriteString(" BOOL")
				}

				boolValue, err := strconv.ParseBool(string(val))
				if err != nil {
					return err
				}

				w.params = append(w.params, boolValue)
			case jsonparser.Number:
				if w.PrepareSQLMap[deviceID] == nil {
					createTableBuffer.WriteString(" DOUBLE")
				}

				floatValue, err := strconv.ParseFloat(string(val), 64)
				if err != nil {
					return err
				}

				w.params = append(w.params, floatValue)
			case jsonparser.String:
				if w.PrepareSQLMap[deviceID] == nil {
					createTableBuffer.WriteString(" NCHAR(100)")
				}

				w.params = append(w.params, string(val))
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

	if w.PrepareSQLMap[deviceID] == nil {
		createTableBuffer.WriteString(")")
		if _, err := w.db.Exec(createTableBuffer.String()); err != nil {
			return err
		}

		// create statments
		schemaBuf.WriteString(")")

		sqlBuf.WriteByte(' ')
		sqlBuf.Write(schemaBuf.Bytes())
		sqlBuf.WriteString(" values (")
		sqlBuf.Write(dataBuf.Bytes())
		sqlBuf.WriteString(")")

		stmt, err := w.db.Prepare(sqlBuf.String())
		if err != nil {
			log.Println(err)
			return err
		}

		w.PrepareSQLMap[deviceID] = stmt
	}

	// 上边是根据数据组成sql语句,之后调用tdengine的库写入数据
	if w.WriteTo {
		if _, err := w.PrepareSQLMap[deviceID].Exec(w.params...); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}
