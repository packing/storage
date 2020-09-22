package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/go-sql-driver/mysql"
    "github.com/packing/nbpy/utils"
)

type MySQL struct {
    db *sql.DB
}

func (mysql *MySQL) InitPool(config MySQLConfig) bool {
    dataSource := fmt.Sprintf("%s:%s@%s/%s", config.User, config.Pwd, config.Addr, config.DBName)
    db, err := sql.Open("mysql", dataSource)
    if err != nil {
        utils.LogError("初始化mysql连接池失败", err)
        return false
    }

    db.SetMaxOpenConns(config.PoolSize)
    db.SetMaxIdleConns(config.PoolSize)

    idle, err := time.ParseDuration(config.IdleTime)
    if err == nil {
        db.SetConnMaxIdleTime(idle)
    } else if config.IdleTime != "" {
        utils.LogWarn("mysql配置节中空闲时长字段idle的配置值可能有误")
    }

    life, err := time.ParseDuration(config.LifeTime)
    if err == nil {
        db.SetConnMaxLifetime(life)
    } else if config.LifeTime != "" {
        utils.LogWarn("mysql配置节中生存时长字段life的配置值可能有误")
    }

    return true
}

func (mysql *MySQL) Query(sql string) [] map[string] interface{} {

    return nil
}

func (mysql *MySQL) QueryWithoutResult(sql string) int {

    return 0
}