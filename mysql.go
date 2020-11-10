package main

import (
    "database/sql"
    "fmt"
    "strings"
    "time"

    _ "github.com/go-sql-driver/mysql"
    "github.com/packing/nbpy/utils"
)

const (
    TXActionNothing = iota
    TXActionInsert
    TXActionUpdate
)

type TxParam struct {
    sql string
    action int
    args []interface{}
}

type MySQL struct {
    db *sql.DB
}

func (mysql *MySQL) InitPool(config MySQLConfig) bool {
    format := "%s:%s@%s/%s?charset=%s"
    if strings.Contains(config.Addr, ":") {
        format = "%s:%s@tcp(%s)/%s?charset=%s"
    }
    dataSource := fmt.Sprintf(format, config.User, config.Pwd, config.Addr, config.DBName, config.Encoding)
    db, err := sql.Open("mysql", dataSource)
    if err != nil {
        utils.LogError("初始化mysql连接池失败", err)
        return false
    }

    db.SetMaxOpenConns(config.PoolSize)
    db.SetMaxIdleConns(config.PoolSize)

    //idle, err := time.ParseDuration(config.IdleTime)
    //if err == nil {
    //    db.SetConnMaxIdleTime(idle)
    //} else if config.IdleTime != "" {
    //    utils.LogWarn("mysql配置节中空闲时长字段idle的配置值可能有误")
    //}

    life, err := time.ParseDuration(config.LifeTime)
    if err == nil {
        db.SetConnMaxLifetime(life)
    } else if config.LifeTime != "" {
        utils.LogWarn("mysql配置节中生存时长字段life的配置值可能有误")
    }

    mysql.db = db
    dataSource = strings.ReplaceAll(dataSource, config.Pwd, "***")
    utils.LogInfo("初始化连接池成功. 容量: %d / %d, DBSource: %s", db.Stats().OpenConnections, db.Stats().MaxOpenConnections, dataSource)

    return true
}

func (mysql *MySQL) ReadRows(rows *sql.Rows) ([] map[string] interface{}, error) {
    cols, err := rows.Columns()
    if err != nil {
        utils.LogError("============= MySQL ReadRows Error =============")
        utils.LogError(">>> Description: %s", err.Error())
        utils.LogError("================================================")
        return [] map[string] interface{}{}, err
    }
    results := make([]map[string] interface{}, 0)
    for rows.Next() {
        rowResult := make([]interface{}, len(cols))
        rowResultPtr := make([]interface{}, len(cols))
        for i := range cols {
            rowResultPtr[i] = &rowResult[i]
        }
        err = rows.Scan(rowResultPtr...)
        if err == nil {
            m := make(map[string] interface{})
            for i, col := range rowResult {
                switch col.(type) {
                case []byte:
                    m[cols[i]] = string(col.([]byte))
                default:
                    m[cols[i]] = col
                }
            }
            results = append(results, m)
        } else {
            utils.LogError("============= MySQL ReadRows Error =============")
            utils.LogError(">>> Description: %s", err.Error())
            utils.LogError("================================================")
            break
        }
    }
    return results, err
}

func (mysql *MySQL) Query(sql string, args ...interface{}) ([] map[string] interface{}, error) {
    rows, err := mysql.db.Query(sql, args...)
    defer func() {
        if rows != nil {
            rows.Close()
        }
    }()

    if err != nil {
        utils.LogError("============= MySQL Query Error =============")
        utils.LogError(">>> Sql: %s", sql)
        if len(args) > 0 {
            utils.LogError(">>> Args: %s", args)
        }
        utils.LogError(">>> Description: %s", err.Error())
        utils.LogError("=============================================")

        return make([] map[string] interface{}, 0), err
    }

    return mysql.ReadRows(rows)
}

func (mysql *MySQL) QueryWithoutResult(sql string, args ...interface{}) (int64, error) {
    result, err := mysql.db.Exec(sql, args...)
    if err != nil {
        utils.LogError("============= MySQL Query Error =============")
        utils.LogError(">>> Sql: %s", sql)
        if len(args) > 0 {
            utils.LogError(">>> Args: ", sql)
        }
        utils.LogError(">>> Description: %s", err.Error())
        utils.LogError("=============================================")
        return 0, err
    }

    lastId, errId := result.LastInsertId()
    effectRows, errRow := result.RowsAffected()

    if errId == nil && lastId > 0 {
        return lastId, nil
    }

    if errRow == nil {
        return effectRows, nil
    }
    return 0, nil
}

func (mysql *MySQL) Transaction(params ...TxParam) (bool, error) {
    tx, err := mysql.db.Begin()
    if err != nil {
        utils.LogError("============= MySQL Query Error =============")
        utils.LogError(">>> Params: ", params)
        utils.LogError(">>> Description: %s", err.Error())
        utils.LogError("=============================================")
        return false, err
    }

    for _, p := range params {
        r, err := tx.Exec(p.sql, p.args...)
        if err != nil {
            tx.Rollback()
            return false, err
        }
        switch p.action {
        case TXActionInsert:
            newId, err := r.LastInsertId()
            if err != nil || newId <= 0 {
                tx.Rollback()
                return false, err
            }
        case TXActionUpdate:
            effRow, err := r.RowsAffected()
            if err != nil || effRow <= 0 {
                tx.Rollback()
                return false, err
            }
        case TXActionNothing:
            continue
        }
    }

    tx.Commit()

    return true, nil
}