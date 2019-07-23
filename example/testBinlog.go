package main

import (
	"log"

	"github.com/jc3wish/Bristol/mysql"
	"time"
	"reflect"
)

func callback(data *mysql.EventReslut) {
	log.Println(data)
	if data.Query == ""{
		for k,v := range data.Rows[len(data.Rows)-1]{
			log.Println(k,"==",v,"(",reflect.TypeOf(v),")")
		}
	}
}

func main() {
	test1()
	return
	filename := "mysql-bin.000022"
	var position uint32 = 13333
	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["testdbcreate"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    "root:root@tcp(127.0.0.1:3306)/test",
		CallbackFun:   callback,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1},
	}
	go BinlogDump.StartDumpBinlog(filename, position, 100,reslut,"",0)
	go func() {
		v := <-reslut
		log.Printf("monitor reslut:%s \r\n", v)
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}

func test1()  {
	filename := "mysql-bin.000004"
	var position uint32 = 107
	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["mysql"] = 1
	DataSource := "root:123456@tcp(45.40.207.2:3311)/mysql"
	BinlogDump := &mysql.BinlogDump{
		DataSource:    DataSource,
		CallbackFun:   callback,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{
			mysql.QUERY_EVENT,
			mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,
			mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2,
			mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,
			},
	}
	go BinlogDump.StartDumpBinlog(filename, position, 100,reslut,"",0)
	go func() {
		v := <-reslut
		log.Printf("monitor reslut:%s \r\n", v)
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}