package main

import (
    "strings"
    "sync"
    "time"

    "github.com/garyburd/redigo/redis"
    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/utils"
)

type IRedis interface {
    InitPool(config RedisConfig)
    OpenConn(uint64) bool
    CloseConn(uint64)
    Do(string, ...interface{}) (interface{}, error)
    Send(uint64, string, ...interface{}) error
    Flush(uint64) error
    Receive(uint64) (interface{}, error)
}

type Redis struct {
    pool *redis.Pool
    forkConns map[uint64] redis.Conn
    mutex sync.Mutex
}

func(r *Redis) InitPool(config RedisConfig) {
    r.forkConns = make(map[uint64] redis.Conn)
    r.pool = new(redis.Pool)
    r.pool.MaxIdle = config.Idle
    r.pool.MaxActive = config.Active
    idle, err := time.ParseDuration(config.IdleTime)
    if err != nil {
        r.pool.IdleTimeout = idle
    }
    life, err := time.ParseDuration(config.LifeTime)
    if err != nil {
        r.pool.MaxConnLifetime = life
    }
    r.pool.Dial = func() (conn redis.Conn, e error) {
        utils.LogInfo("Redis 连接至 %s", config.Addr)
        var ops = make([]redis.DialOption, 0)

        if config.Pwd != "" {
            ops = append(ops, redis.DialPassword(config.Pwd))
        }

        if strings.Contains(config.Addr, ":") {
            return redis.Dial("tcp", config.Addr, ops...)
        } else {
            return redis.Dial("unix", config.Addr, ops...)
        }
    }

    r.pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
        if time.Since(t) < time.Minute {
            return nil
        }
        _, err := c.Do("PING")
        if nil != err {
            utils.LogError("Redis连接的 ping 操作返回错误: %s ", err.Error())
        }
        return err
    }
    utils.LogInfo("初始化Redis连接池成功. 容量: %d / %d", r.pool.Stats().ActiveCount, r.pool.Stats().IdleCount)
}

func(r *Redis) CloseConn(key uint64) {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    c, ok := r.forkConns[key]
    if ok {
        c.Close()
        delete(r.forkConns, key)
    }
}

func (r *Redis) OpenConn(key uint64) bool {
    return r.forkConn(key) != nil
}

func(r *Redis) forkConn(key uint64) redis.Conn {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    c, ok := r.forkConns[key]
    if !ok {
        c = r.pool.Get()
        r.forkConns[key] = c
    }
    return c
}

func(r *Redis) Do(cmd string, args ...interface{}) (interface{}, error) {
    c := r.pool.Get()
    defer c.Close()

    switch strings.ToLower(cmd) {
    case "set":
        if len(args) > 1 {
            args[1] = r.packData(args[1])
        }
    }

    ret, err := c.Do(cmd, args...)

    switch strings.ToLower(cmd) {
    case "get":
        if ret != nil {
            ret = r.unPackData(ret)
        }
    }

    return ret, err
}

func(r *Redis) Send(key uint64, cmd string, args ...interface{}) error {
    c := r.forkConn(key)
    err := c.Send(cmd, args...)
    return err
}

func(r *Redis) Flush(key uint64) error {
    c := r.forkConn(key)
    return c.Flush()
}

func(r *Redis) Receive(key uint64) (interface{}, error) {
    c := r.forkConn(key)
    return c.Receive()
}

func(r *Redis) unPackData(v interface{}) interface{} {
    var b []byte = nil
    switch v.(type) {
    case []byte:
        b = v.([]byte)
    case string:
        return v
    }

    if b == nil || len(b) == 0 {
        return nil
    }

    err, data, _ := codecs.CodecIMv2.Decoder.Decode(b)
    if err == nil {
        return data
    }
    return nil
}

func(r *Redis) packData(v interface{}) string {
    err, data := codecs.CodecIMv2.Encoder.Encode(&v)
    if err == nil {
        return string(data)
    }
    return ""
}