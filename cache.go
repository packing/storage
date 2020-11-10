package main

import (
    "fmt"
    "reflect"
    "strings"
    "sync"
    "time"
)

const (
    REDIS_COMMAND_GET = "get"
    REDIS_COMMAND_SET = "set"
    REDIS_COMMAND_SETNX = "setnx"
    REDIS_COMMAND_SETEX = "setex"
    REDIS_COMMAND_STRLEN = "strlen"
    REDIS_COMMAND_INCR = "incr"
    REDIS_COMMAND_INCRBY = "incrby"
    REDIS_COMMAND_DECR = "decr"
    REDIS_COMMAND_DECRBY = "decrby"
    REDIS_COMMAND_APPEND = "append"
    REDIS_COMMAND_DEL = "del"
    REDIS_COMMAND_MGET = "mget"
    REDIS_COMMAND_MSET = "mset"
    REDIS_COMMAND_HGET = "hget"
    REDIS_COMMAND_HSET = "hset"
    REDIS_COMMAND_HDEL = "hdel"
    REDIS_COMMAND_HMGET = "hmget"
    REDIS_COMMAND_HMSET = "hmset"
    REDIS_COMMAND_HSETNX = "hsetnx"
    REDIS_COMMAND_HGETALL = "hgetall"
    REDIS_COMMAND_HEXISTS = "hexists"
    REDIS_COMMAND_HKEYS = "hkeys"
    REDIS_COMMAND_HVALS = "hvals"
    REDIS_COMMAND_HLEN = "hlen"
    REDIS_COMMAND_LLEN = "llen"
    REDIS_COMMAND_LPOP = "lpop"
    REDIS_COMMAND_RPOP = "rpop"
    REDIS_COMMAND_LPUSH = "lpush"
    REDIS_COMMAND_RPUSH = "rpush"
    REDIS_COMMAND_LSET = "lset"
    REDIS_COMMAND_LINSERT = "linsert"
    REDIS_COMMAND_LINDEX = "lindex"
    REDIS_COMMAND_LREM = "lrem"
    REDIS_COMMAND_LRANGE = "lrange"
    REDIS_COMMAND_LTRIM = "ltrim"
    REDIS_COMMAND_SADD = "sadd"
    REDIS_COMMAND_SCARD = "scard"
    REDIS_COMMAND_SDIFF = "sdiff"
    REDIS_COMMAND_SINTER = "sinter"
    REDIS_COMMAND_SISMEMBER = "sismember"
    REDIS_COMMAND_SMEMBERS = "smembers"
    REDIS_COMMAND_SPOP = "spop"
    REDIS_COMMAND_SRANDMEMBER = "srandmember"
    REDIS_COMMAND_SREM = "srem"
    REDIS_COMMAND_SUNION = "sunion"
    REDIS_COMMAND_ZADD = "zadd"
    REDIS_COMMAND_ZCARD = "zcard"
    REDIS_COMMAND_ZCOUNT = "zcount"
    REDIS_COMMAND_ZINCRBY = "zincrby"
    REDIS_COMMAND_ZRANGE = "zrange"
    REDIS_COMMAND_ZRANK = "zrank"
    REDIS_COMMAND_ZREM = "zrem"
    REDIS_COMMAND_ZREMRANGEBYRANK = "zremrangebyrank"
    REDIS_COMMAND_ZREMRANGEBYSCORE = "zremrangebyscore"
    REDIS_COMMAND_ZREVRANK = "zrevrank"
    REDIS_COMMAND_ZSCORE = "zscore"
)

type IPoolData interface {
    SetValue(interface{})
    SetKeyValue(string, interface{})
    SetIndexValue(int, interface{})
    GetValue() interface{}
    GetKeyValue(string) interface{}
    GetIndexValue(int) interface{}
    GetLength() int
    GetKeys() []interface{}
    SetLifeCycle(int64)
    CheckAlive() bool
}

type StandardData struct {
    data   interface{}
    expire int64
    mutex  sync.Mutex
}

func (s *StandardData) CheckAlive() bool {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    return s.expire == 0 || s.expire > time.Now().UnixNano()
}

func (s *StandardData) SetLifeCycle(e int64) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.expire = e
}

func (s *StandardData) SetValue(d interface{}) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.data = d
}

func (s *StandardData) SetKeyValue(string, interface{}) {}
func (s *StandardData) SetIndexValue(int, interface{})  {}
func (s *StandardData) GetValue() interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    return s.data
}

func (s *StandardData) GetKeyValue(string) interface{} { return nil }
func (s *StandardData) GetIndexValue(int) interface{}  { return nil }
func (s *StandardData) GetLength() int                 { return -1 }
func (s *StandardData) GetKeys() []interface{}         { return nil }

type MapData struct {
    data   map[string]interface{}
    expire int64
    mutex  sync.Mutex
}

func (s *MapData) check() {
    if s.data == nil {
        s.data = make(map[string]interface{})
    }
}

func (s *MapData) CheckAlive() bool {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    return s.expire == 0 || s.expire > time.Now().UnixNano()
}
func (s *MapData) SetLifeCycle(e int64) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.expire = e
}

func (s *MapData) SetValue(interface{}) {}
func (s *MapData) SetKeyValue(key string, val interface{}) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    s.data[key] = val
}
func (s *MapData) SetIndexValue(int, interface{}) {}
func (s *MapData) GetValue() interface{}          { return nil }
func (s *MapData) GetKeyValue(key string) interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    v, _ := s.data[key]
    return v
}
func (s *MapData) GetIndexValue(int) interface{} { return nil }
func (s *MapData) GetLength() int {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    return len(s.data)
}
func (s *MapData) GetKeys() []interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    vs := reflect.ValueOf(s.data).MapKeys()
    ks := make([]interface{}, len(vs))
    for i, v := range vs {
        ks[i] = v.Interface()
    }
    return ks
}

type ListData struct {
    data   [] interface{}
    expire int64
    mutex  sync.Mutex
}

func (s *ListData) check() {
    if s.data == nil {
        s.data = make([] interface{}, 0)
    }
}

func (s *ListData) CheckAlive() bool {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    return s.expire == 0 || s.expire > time.Now().UnixNano()
}
func (s *ListData) SetLifeCycle(e int64) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.expire = e
}
func (s *ListData) SetValue(interface{})                    {}
func (s *ListData) SetKeyValue(key string, val interface{}) {}
func (s *ListData) SetIndexValue(i int, val interface{}) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    if i >= 0 && i < len(s.data) {
        s.data[i] = val
    } else {
        s.data = append(s.data, val)
    }
}

func (s *ListData) GetValue() interface{}              { return nil }
func (s *ListData) GetKeyValue(key string) interface{} { return nil }
func (s *ListData) GetIndexValue(i int) interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    if i >= 0 && i < len(s.data) {
        return s.data[i]
    }
    return nil
}

func (s *ListData) GetLength() int {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    return len(s.data)
}
func (s *ListData) GetKeys() []interface{} { return nil }


type LocalFastRedis struct {
    dataPool map[string] IPoolData
    mutex sync.Mutex
}

func(r *LocalFastRedis) OpenConn(uint64) bool {
    return true
}

func(r *LocalFastRedis) CloseConn(uint64) { }

func(r *LocalFastRedis) InitPool(config RedisConfig) {
    r.dataPool = make(map[string]IPoolData)
}

func(r *LocalFastRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
    switch strings.ToLower(cmd) {

    }
    return nil, nil
}

func(r *LocalFastRedis) Send(uint64, string, ...interface{}) error {
    return nil
}

func(r *LocalFastRedis) Flush(uint64) error {
    return nil
}

func(r *LocalFastRedis) Receive(uint64) (interface{}, error) {
    return nil, nil
}


func(r *LocalFastRedis) getData(k string) IPoolData {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    d, ok := r.dataPool[k]
    if ok {
        if !d.CheckAlive() {
            delete(r.dataPool, k)
            return nil
        }
    }
    return d
}

func(r *LocalFastRedis) ensureStandardData(k string) IPoolData {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    d, ok := r.dataPool[k]
    if !ok {
        d = new(StandardData)
        r.dataPool[k] = d
    }
    return d
}

func(r *LocalFastRedis) ensureMapData(k string) IPoolData {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    d, ok := r.dataPool[k]
    if !ok {
        d = new(MapData)
        r.dataPool[k] = d
    }
    return d
}

func(r *LocalFastRedis) ensureListData(k string) IPoolData {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    d, ok := r.dataPool[k]
    if !ok {
        d = new(ListData)
        r.dataPool[k] = d
    }
    return d
}

func(r *LocalFastRedis) findData(k string) (interface{}, error) {
    data := r.getData(k)
    if data != nil {
        return data.GetValue(), nil
    } else {
        return nil, fmt.Errorf("no data found for this key")
    }
}

func(r *LocalFastRedis) setLifeCycle(k string, e int64) {
    data := r.getData(k)
    if data != nil {
        data.SetLifeCycle(e)
    }
}

func(r *LocalFastRedis) setData(k string, d interface{}) {
    data := r.ensureStandardData(k)
    data.SetValue(d)
}

func(r *LocalFastRedis) setMapData(k string, key string, d interface{}) {
    data := r.ensureMapData(k)
    data.SetKeyValue(key, d)
}

func(r *LocalFastRedis) getMapData(k string, key string) interface{} {
    data := r.getData(k)
    if data != nil {
        return data.GetKeyValue(key)
    } else {
        return nil
    }
}

func(r *LocalFastRedis) setListData(k string, index int, d interface{}) {
    data := r.ensureListData(k)
    data.SetIndexValue(index, d)
}

func(r *LocalFastRedis) getListData(k string, index int) interface{} {
    data := r.getData(k)
    if data != nil {
        return data.GetIndexValue(index)
    } else {
        return nil
    }
}