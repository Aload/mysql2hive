package model

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

/**
 *db config
 */
type DbConf struct {
	DbType     string `json:"db_type"`
	IpAddr     string `json:"ip_addr"`
	DbName     string `json:"db_name"`
	DbUser     string `json:"db_user"`
	DbPwd      string `json:"db_pwd"`
	OutputFile string `json:"output_file"`
}

/**
 *解析配置文件
 */
func ParseConf(path string) (conf *DbConf) {
	conf = new(DbConf)
	bytes, _ := ioutil.ReadFile(path)
	if err := json.Unmarshal(bytes, conf); err != nil {
		log.Fatalf("parse db conf is error %v", err)
	}
	return conf
}
