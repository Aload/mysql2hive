package main

import (
	"domin"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"io/ioutil"
	"model"
	"os"
	"strings"
	"sync"
)

var (
	syn         *sync.WaitGroup
	conf        *model.DbConf
	connectPool = make(map[string]*xorm.Engine)
)

func init() {

	conf = model.ParseConf("src/res/dbConf.json")
	syn = new(sync.WaitGroup)
}

//sqlType, hostName, dbName, userName, pwd string
func main() {
	strName := strings.Split(conf.DbName, ",")
	connectPool, _ = domin.NewSession(conf.DbType, conf.IpAddr, conf.DbName, conf.DbUser, conf.DbPwd)
	db1 := connectPool[strName[1]]
	resultsSlice, _ := db1.Query("SELECT TABLE_NAME FROM `TABLES` WHERE TABLE_SCHEMA='iebm_platform_v2_db_dev'")
	for _, sliceData := range resultsSlice {
		col := sliceData["TABLE_NAME"]
		if !strings.Contains(string(col), "biz") {
			continue
		}
		syn.Add(1)
		go func(column []byte) {
			var count int
			defer syn.Done()
			sql := fmt.Sprintf("select %v, %v, %v, %v from COLUMNS where TABLE_SCHEMA='%v' and TABLE_NAME='%v'", "COLUMN_NAME", "DATA_TYPE", "COLUMN_COMMENT", "COLUMN_TYPE", "iebm_platform_v2_db_yf_test", string(col))
			slices, _ := db1.Query(sql)
			len := len(slices)
			var creatDb string = "CREATE DATABASE IF NOT EXISTS zhyb_operation;"
			creatTb := fmt.Sprintf("CREATE TABLE IF NOT EXISTS zhyb_operation.%v (\t", string(col))
			initStr := fmt.Sprintf("%v \r\n %v", creatDb, creatTb)
			//长度一致
			for _, column := range slices {
				count++
				columnName := string(column["COLUMN_NAME"])
				dataType := string(column["DATA_TYPE"])
				commentDetail := string(column["COLUMN_COMMENT"])
				columnType := string(column["COLUMN_TYPE"])
				//user_id string			    ,--用户ID
				str := formatDB2Hive(columnName, dataType, commentDetail, columnType, count, len)
				fmt.Println(str)
				if str == "" {
					continue
				}
				initStr = fmt.Sprintf("%v \r\n %v", initStr, str)
			}
			parStr := fmt.Sprintf(")PARTITIONED BY (%v string)", "date")
			storeStr := fmt.Sprintf("ROW FORMAT DELIMITED FIELDS TERMINATED BY '%v' 	STORED AS %v;", ",", "TEXTFILE")
			footerStr := fmt.Sprintf("%v \r\n %v", parStr, storeStr)
			hiveSql := fmt.Sprintf("%v \r\n %v", initStr, footerStr)
			fmt.Println("--------------------------------------------------")
			fmt.Println(hiveSql)
			ioutil.WriteFile(conf.OutputFile+"/"+string(col)+".hql", []byte(hiveSql), os.FileMode(os.ModeAppend))
			fmt.Println("--------------------------------------------------")
		}(col)
	}
	syn.Wait()
}

/**
 *db struct change to hive
*	create database if not exists bdm;
	create external table if not exists bdm.user(
	user_id string			    ,--用户ID
	user_name string			,--用户登陆名
	is_married bigint			,--婚姻状况
	education string			,--学历
	monthly_money double		,--收入
	profession string			--职业
	) partitioned by (dt string)
	row format delimited fields terminated by ',';
: TINYINT
  | SMALLINT
  | INT
  | BIGINT
  | BOOLEAN
  | FLOAT
  | DOUBLE
  | DOUBLE PRECISION -- (Note: Available in Hive 2.2.0 and later)
  | STRING
  | BINARY      -- (Note: Available in Hive 0.8.0 and later)
  | TIMESTAMP   -- (Note: Available in Hive 0.8.0 and later)
  | DECIMAL     -- (Note: Available in Hive 0.11.0 and later)
  | DECIMAL(precision, scale)  -- (Note: Available in Hive 0.13.0 and later)
  | DATE        -- (Note: Available in Hive 0.12.0 and later)
  | VARCHAR     -- (Note: Available in Hive 0.12.0 and later)
  | CHAR        -- (Note: Available in Hive 0.13.0 and later)

`ip` string COMMENT 'remote real ip',
*/
func formatDB2Hive(columnName, dataType, comment, columnType string, count, lens int) string {
	if columnName == "" || dataType == "" || len(columnName) == 0 || len(dataType) == 0 {
		return ""
	}
	var tmp = strings.ToUpper(columnType)
	switch dataType {
	case "text", "blob", "longblob", "set", "mediumtext", "bit", "varbinary", "char", "longtext", "varchar":
		tmp = "STRING"
	case "date", "time", "timestamp":
		tmp = "TIMESTAMP"
	case "float", "double", "decimal":
		tmp = "DOUBLE"
	case "int", "bigint", "mediumint":
		tmp = "INT"
	}
	join := strings.Join([]string{columnName, tmp}, "\t")
	if comment == "" {
		return join
	}
	if lens == count {
		return strings.Join([]string{join, "'"+comment+"'"}, "\tCOMMENT\t")
	} else {
		return strings.Join([]string{join, "'"+comment+"',"}, "\tCOMMENT\t")
	}
}
