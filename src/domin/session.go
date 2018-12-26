package domin

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"log"
	"strings"
	"time"
)

var (
	confPool = make(map[string]*xorm.Engine)
	engine   *xorm.Engine
)

/**
创建数据库连接
*/
func NewSession(sqlType, hostName, dbName, userName, pwd string) (col map[string]*xorm.Engine, err error) {
	strName := strings.Split(dbName, ",")
	for _, name := range strName {
		if sqlType == "mysql" {
			if confPool[name] != nil {
				return confPool, nil
			}
			dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8&loc=Local", userName, pwd, hostName, 3306, name)
			if engine, err = xorm.NewEngine(sqlType, dsn); err != nil {
				log.Fatalf("Open %v connection failed:%v", sqlType, err.Error())
				return nil, err
			}
		}
		if err = engine.Ping(); err != nil {
			log.Fatalf("%s", err.Error())
			return nil, nil
		}
		engine.SetConnMaxLifetime(time.Hour * 72)
		engine.SetMaxIdleConns(63553)
		engine.SetMaxIdleConns(63553)
		confPool[name] = engine
	}
	return confPool, nil
}
