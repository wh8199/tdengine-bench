package main

import (
	"net/http"
	"sync"

	_ "net/http/pprof"

	_ "github.com/taosdata/driver-go/taosSql"
)

var (
	url = "root:taosdata/tcp(127.0.0.1:6030)/"
)

func main() {
	go func() {
		http.ListenAndServe(":8888", nil)
	}()

	ch := make(chan []byte, 100)

	s := Simulator{
		ReportInterval: "1s",     // 单个模拟器发送数据的时间间隔
		Concurrent:     1,        // 模拟器的数量
		ReportCount:    100,      // 每个模拟器发送的总数量
		PropertyCount:  100,      // 模拟器中每个设备的属性个数
		Prefix:         "device", //模拟器设备名字前缀
	}

	o := Output{
		Concurrent:      1,      // tdengine写入goroutine的数量
		Database:        "data", // tdengine的数据库名字
		DSN:             url,    // tdengine的地址
		DataChan:        make(chan []byte, 100),
		WriteToTDengine: true, // 控制是否写入数据库，为true时写入，为false时不写入
		Batch:           10,
	}

	o.Start(ch, url)
	s.Start(ch)

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
