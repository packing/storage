package main

import (
    "fmt"
    "github.com/packing/nbpy/utils"
    "math/rand"
    "strconv"
    "strings"
    "sync"
    "time"

    set "github.com/deckarep/golang-set"

    "github.com/packing/nbpy/bits"
    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/errors"

)

//goland:noinspection ALL
const (
    REDIS_TYPE_STANDARD = 0
    REDIS_TYPE_MAP = 1
    REDIS_TYPE_LIST = 2
    REDIS_TYPE_SET = 3
)

type RedisDataType uint8

//goland:noinspection ALL
const (
    REDIS_COMMAND_GET              = "get"
    REDIS_COMMAND_SET              = "set"
    REDIS_COMMAND_GETSET           = "getset"
    REDIS_COMMAND_SETNX            = "setnx"
    REDIS_COMMAND_SETEX            = "setex"
    REDIS_COMMAND_STRLEN           = "strlen"
    REDIS_COMMAND_INCR             = "incr"
    REDIS_COMMAND_INCRBY           = "incrby"
    REDIS_COMMAND_DECR             = "decr"
    REDIS_COMMAND_DECRBY           = "decrby"
    REDIS_COMMAND_APPEND           = "append"
    REDIS_COMMAND_DEL              = "del"
    REDIS_COMMAND_MGET             = "mget"
    REDIS_COMMAND_MSET             = "mset"
    REDIS_COMMAND_HGET             = "hget"
    REDIS_COMMAND_HSET             = "hset"
    REDIS_COMMAND_HDEL             = "hdel"
    REDIS_COMMAND_HMGET            = "hmget"
    REDIS_COMMAND_HMSET            = "hmset"
    REDIS_COMMAND_HSETNX           = "hsetnx"
    REDIS_COMMAND_HGETALL          = "hgetall"
    REDIS_COMMAND_HEXISTS          = "hexists"
    REDIS_COMMAND_HKEYS            = "hkeys"
    REDIS_COMMAND_HVALS            = "hvals"
    REDIS_COMMAND_HLEN             = "hlen"
    REDIS_COMMAND_LLEN             = "llen"
    REDIS_COMMAND_LPOP             = "lpop"
    REDIS_COMMAND_RPOP             = "rpop"
    REDIS_COMMAND_LPUSH            = "lpush"
    REDIS_COMMAND_RPUSH            = "rpush"
    REDIS_COMMAND_LSET             = "lset"
    REDIS_COMMAND_LINSERT          = "linsert"
    REDIS_COMMAND_LINSERTAT        = "linsertat"
    REDIS_COMMAND_LINDEX           = "lindex"
    REDIS_COMMAND_LREM             = "lrem"
    REDIS_COMMAND_LREMAT           = "lremat"
    REDIS_COMMAND_LRANGE           = "lrange"
    REDIS_COMMAND_LTRIM            = "ltrim"
    REDIS_COMMAND_SADD             = "sadd"
    REDIS_COMMAND_SCARD            = "scard"
    REDIS_COMMAND_SDIFF            = "sdiff"
    REDIS_COMMAND_SDIFFSTORE       = "sdiffstore"
    REDIS_COMMAND_SINTER           = "sinter"
    REDIS_COMMAND_SINTERSTORE      = "sinterstore"
    REDIS_COMMAND_SISMEMBER        = "sismember"
    REDIS_COMMAND_SMEMBERS         = "smembers"
    REDIS_COMMAND_SPOP             = "spop"
    REDIS_COMMAND_SRANDMEMBER      = "srandmember"
    REDIS_COMMAND_SREM             = "srem"
    REDIS_COMMAND_SUNION           = "sunion"
    REDIS_COMMAND_SUNIONSTORE      = "sunionstore"
    REDIS_COMMAND_ZADD             = "zadd"
    REDIS_COMMAND_ZCARD            = "zcard"
    REDIS_COMMAND_ZCOUNT           = "zcount"
    REDIS_COMMAND_ZINCRBY          = "zincrby"
    REDIS_COMMAND_ZRANGE           = "zrange"
    REDIS_COMMAND_ZRANK            = "zrank"
    REDIS_COMMAND_ZREM             = "zrem"
    REDIS_COMMAND_ZREMRANGEBYRANK  = "zremrangebyrank"
    REDIS_COMMAND_ZREMRANGEBYSCORE = "zremrangebyscore"
    REDIS_COMMAND_ZREVRANK         = "zrevrank"
    REDIS_COMMAND_ZSCORE           = "zscore"
)

var ErrorArgsLength = errors.Errorf("parameter number mismatch")
var ErrorKeyNotFound = errors.Errorf("the specified key does not exist")
var ErrorTypeNotMatch = errors.Errorf("the data type of the specified key do not match")


type SetData struct {
    data   set.Set
    expire int64
}
type IPoolData interface {
    GetDataType() RedisDataType
    SetValue(interface{})
    SetKeyValue(interface{}, interface{})
    SetIndexValue(int, interface{})
    GetValue() interface{}
    GetKeyValue(interface{}) interface{}
    GetIndexValue(int) interface{}
    GetLength() int
    GetKeys() []interface{}
    GetValues() []interface{}
    SetLifeCycle(int64)
    CheckAlive() bool
    HasKey(interface{}) bool
    DelKey(interface{}) bool
    PopValue(int) interface{}
    PopValues(int) []interface{}
    PeekValues(int) []interface{}
    PopValueByValue(int, interface{}) int
    InsertValue(int, interface{})
    AppendValue(interface{})
    InsertValueByValue(int, interface{}, interface{})
    Slice(int, int)
    Add(interface{}) bool
    Remove(interface{}) bool
    RemoveMutil(...interface{}) int
    Diff(...IPoolData) []interface{}
    Inter(...IPoolData) []interface{}
    Union(...IPoolData) []interface{}
    Contains(interface{}) bool
    BuildSet(...interface{})
    GetSrcData() interface{}
    Incr(int64)
}

type StandardData struct {
    data   interface{}
    expire int64
    mutex  sync.Mutex
}

func (s *StandardData) GetDataType() RedisDataType {
    return REDIS_TYPE_STANDARD
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

func (s *StandardData) GetValue() interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    return s.data
}

func (s *StandardData) Incr(add int64) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    var nData int64 = 0
    var err error
    isStrValue := false
    if s.data != nil {
        strData, ok := s.data.(string)
        if ok {
            isStrValue = true
            nData, err = strconv.ParseInt(strData, 10, 64)
            if err != nil {
                nData = 0
            }
        } else {
            nData = codecs.Int64FromInterface(s.data)
        }
    } else {
    }
    nData += add
    if isStrValue {
        s.data = strconv.FormatInt(nData, 10)
    } else {
        s.data = nData
    }
}

func (s *StandardData) AppendValue(interface{})                             {}
func (s *StandardData) RemoveMutil(...interface{}) int                      { return -1 }
func (s *StandardData) PeekValues(int) []interface{}                        { return [] interface{}{} }
func (s *StandardData) Contains(interface{}) bool                           { return false }
func (s *StandardData) BuildSet(...interface{})                             {}
func (s *StandardData) GetSrcData() interface{}                             { return s.data }
func (s *StandardData) Inter(...IPoolData) []interface{}                    { return [] interface{}{} }
func (s *StandardData) Diff(...IPoolData) []interface{}                     { return [] interface{}{} }
func (s *StandardData) Union(...IPoolData) []interface{}                    { return [] interface{}{} }
func (s *StandardData) Add(interface{}) bool                                { return false }
func (s *StandardData) Remove(interface{}) bool                             { return false }
func (s *StandardData) Slice(int, int)                                      {}
func (s *StandardData) InsertValueByValue(int, interface{}, interface{})    {}
func (s *StandardData) PopValueByValue(int, interface{}) int                { return -1 }
func (s *StandardData) InsertValue(int, interface{})                        {}
func (s *StandardData) PopValue(int) interface{}                            { return nil }
func (s *StandardData) PopValues(int) []interface{}                         { return [] interface{}{} }
func (s *StandardData) SetKeyValue(interface{}, interface{})                {}
func (s *StandardData) SetIndexValue(int, interface{})                      {}
func (s *StandardData) GetKeyValue(interface{}) interface{}                 { return nil }
func (s *StandardData) GetIndexValue(int) interface{}                       { return nil }
func (s *StandardData) GetLength() int                                      { return -1 }
func (s *StandardData) HasKey(interface{}) bool                             { return false }
func (s *StandardData) DelKey(interface{}) bool                             { return false }
func (s *StandardData) GetKeys() []interface{}                              { return nil }
func (s *StandardData) GetValues() []interface{}                            { return nil }

type MapData struct {
    data   *sync.Map
    expire int64
    mutex  sync.Mutex
}

func (s *MapData) check() {
    if s.data == nil {
        s.data = new(sync.Map)
    }
}

func (s *MapData) GetDataType() RedisDataType {
    return REDIS_TYPE_MAP
}

func (s *MapData) DelKey(k interface{}) bool {
    _, ok := s.data.Load(k)
    s.data.Delete(k)
    return ok
}

func (s *MapData) HasKey(k interface{}) bool {
    s.check()
    _, ok := s.data.Load(k)
    return ok
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

func (s *MapData) SetKeyValue(key interface{}, val interface{}) {
    s.check()
    s.data.Store(key, val)
}
func (s *MapData) GetKeyValue(key interface{}) interface{} {
    s.check()
    v, _ := s.data.Load(key)
    return v
}
func (s *MapData) GetLength() int {
    s.check()
    c := 0
    s.data.Range(func(key, value interface{}) bool {
        c += 1
        return true
    })
    return c
}
func (s *MapData) GetKeys() []interface{} {
    s.check()
    ks := make([]interface{}, 0)
    s.data.Range(func(key, value interface{}) bool {
        ks = append(ks, key)
        return true
    })
    return ks
}

func (s *MapData) GetValues() []interface{} {
    s.check()
    ks := make([]interface{}, 0)
    s.data.Range(func(key, value interface{}) bool {
        ks = append(ks, value)
        return true
    })
    return ks
}

func (s *MapData) AppendValue(interface{})                             {}
func (s *MapData) RemoveMutil(...interface{}) int                      { return -1 }
func (s *MapData) PeekValues(int) []interface{}                        { return [] interface{}{} }
func (s *MapData) Contains(k interface{}) bool                          { return s.HasKey(k) }
func (s *MapData) Incr(int64)                                           {}
func (s *MapData) BuildSet(...interface{})                              {}
func (s *MapData) GetSrcData() interface{}                              { return s.data }
func (s *MapData) Union(...IPoolData) []interface{}                     { return [] interface{}{} }
func (s *MapData) Inter(...IPoolData) []interface{}                     { return [] interface{}{} }
func (s *MapData) Diff(...IPoolData) []interface{}                      { return [] interface{}{} }
func (s *MapData) Add(interface{}) bool                                 { return false }
func (s *MapData) Remove(interface{}) bool                              { return false }
func (s *MapData) InsertValueByValue(int, interface{}, interface{})     {}
func (s *MapData) PopValueByValue(int, interface{}) int                 { return 0 }
func (s *MapData) Slice(int, int)                                       {}
func (s *MapData) InsertValue(int, interface{})                         {}
func (s *MapData) PopValue(int) interface{}                             { return nil }
func (s *MapData) PopValues(int) []interface{}                          { return [] interface{}{} }
func (s *MapData) SetValue(interface{})                                 {}
func (s *MapData) SetIndexValue(int, interface{})                       {}
func (s *MapData) GetValue() interface{}                                { return nil }
func (s *MapData) GetIndexValue(int) interface{}                        { return nil }

type ListData struct {
    data   [] interface{}
    expire int64
    mutex  sync.Mutex
    sync.Cond
}

func (s *ListData) check() {
    if s.data == nil {
        s.data = make([] interface{}, 0)
    }
}

func (s *ListData) GetDataType() RedisDataType {
    return REDIS_TYPE_LIST
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

func (s *ListData) GetIndexValue(i int) interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    fmt.Println(s.data)
    fmt.Println(i)
    fmt.Println(s.data[i])
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

func (s *ListData) PopValue(at int) interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    var ret interface{} = nil
    if at < 0 {
        at = len(s.data) + at
    }
    if at < len(s.data) {
        ret = s.data[at]
        l1 := s.data[:at]
        l2 := s.data[at + 1:]
        s.data = append(l1, l2...)
    }
    return ret
}

func (s *ListData) InsertValue(at int, v interface{}) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    rat := at
    if rat < 0 {
        rat = len(s.data) + rat
    }
    if rat <= len(s.data) {
        l1 := s.data[:rat]
        l2 := s.data[rat:]
        s.data = append(l1, append([]interface{}{v}, l2...)...)
        fmt.Println(s.data)
    }
}

func (s *ListData) GetValues() []interface{} {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    return s.data
}

func (s *ListData) Slice(start int, end int) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    s.data = s.data[start:end]
}

func (s *ListData) InsertValueByValue(rat int, tv interface{}, nv interface{}) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    at := -1
    for i, v := range s.data {
        if v == tv {
            at = i
            break
        }
    }
    if at >= 0 {
        if rat > 0 {
            at += 1
        }
        if at >= len(s.data) {
            at = -1
        }
        s.InsertValue(at, nv)
    }
}

func (s *ListData) PopValueByValue(count int, tv interface{}) int {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    at := -1
    c := 1
    countAbs := bits.AbsForInt32(count)
    if countAbs == 0 {
        countAbs = len(s.data)
    }
    for {
        if countAbs <= c {
            break
        }
        for i := 0; i < len(s.data); i ++ {
            index := i
            if count < 0 {
                index = len(s.data) + index
            }
            v := s.data[index]
            if v == tv {
                at = i
                break
            }
        }
        if at >= 0 {
            s.PopValue(at)
            c += 1
        } else {
           break
        }
    }
    return c
}

func (s *ListData) AppendValue(v interface{}) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.check()
    s.data = append(s.data, v)
}

func (s *ListData) RemoveMutil(...interface{}) int                      { return -1 }
func (s *ListData) PeekValues(int) []interface{}                        { return [] interface{}{} }
func (s *ListData) PopValues(int) []interface{}                         { return [] interface{}{} }
func (s *ListData) Contains(interface{}) bool                           { return false }
func (s *ListData) Incr(int64)                                          {}
func (s *ListData) BuildSet(...interface{})                             {}
func (s *ListData) GetSrcData() interface{}                             { return s.data }
func (s *ListData) Union(...IPoolData) []interface{}                    { return [] interface{}{} }
func (s *ListData) Inter(...IPoolData) []interface{}                    { return [] interface{}{} }
func (s *ListData) Diff(...IPoolData) []interface{}                     { return [] interface{}{} }
func (s *ListData) Add(interface{}) bool                                { return false }
func (s *ListData) Remove(interface{}) bool                             { return false }
func (s *ListData) SetValue(interface{})                            {}
func (s *ListData) SetKeyValue(key interface{}, val interface{})    {}
func (s *ListData) GetValue() interface{}                           { return nil }
func (s *ListData) GetKeyValue(key interface{}) interface{}         { return nil }
func (s *ListData) HasKey(interface{}) bool                         { return false }
func (s *ListData) DelKey(interface{}) bool                         { return false }
func (s *ListData) GetKeys() []interface{}                          { return nil }


func (s *SetData) check() {
    if s.data == nil {
        s.data = set.NewSet()
    }
}

func (s *SetData) GetDataType() RedisDataType {
    return REDIS_TYPE_SET
}

func (s *SetData) CheckAlive() bool {
    return s.expire == 0 || s.expire > time.Now().UnixNano()
}

func (s *SetData) SetLifeCycle(e int64) {
    s.expire = e
}

func (s *SetData) Add(v interface{}) bool {
    s.check()
    return s.data.Add(v)
}

func (s *SetData) Remove(v interface{}) bool {
    s.check()
    ret := s.data.Contains(v)
    s.data.Remove(v)
    return ret
}

func (s *SetData) GetLength() int {
    s.check()
    return s.data.Cardinality()
}

func (s *SetData) Diff(tos ...IPoolData) []interface{} {
    s.check()
    var diffSet = s.data
    for _, to := range tos {
        sto, ok := to.GetSrcData().(set.Set)
        if ok {
            diffSet = diffSet.Difference(sto)
        }
    }
    return diffSet.ToSlice()
}

func (s *SetData) Inter(tos ...IPoolData) []interface{} {
    s.check()
    var interSet = s.data
    for _, to := range tos {
        sto, ok := to.GetSrcData().(set.Set)
        if ok {
            interSet = interSet.Intersect(sto)
        }
    }
    return interSet.ToSlice()
}

func (s *SetData) Union(tos ...IPoolData) []interface{} {
    s.check()
    var unionSet = s.data
    for _, to := range tos {
        sto, ok := to.GetSrcData().(set.Set)
        if ok {
            unionSet = unionSet.Union(sto)
        }
    }
    return unionSet.ToSlice()
}

func (s *SetData) BuildSet(e ...interface{}) {
    s.check()
    s.data = set.NewSet(e...)
}

func (s *SetData) Contains(v interface{}) bool {
    s.check()
    return s.data.Contains(v)
}

func (s *SetData) GetValues() []interface{} {
    s.check()
    return s.data.ToSlice()
}

func (s *SetData) PopValues(count int) []interface{} {
    s.check()
    var ret = make([]interface{}, 0)
    for i := 0; i < count; i ++ {
        if s.data.Cardinality() == 0 {
            break
        }
        ret = append(ret,s.data.Pop())
    }
    return ret
}
func (s *SetData) PeekValues(count int) []interface{} {
    s.check()
    l := s.data.ToSlice()
    limit := count
    if limit > len(l) {
        limit = len(l)
    }
    if limit == 0 {
        limit = 1
    }
    if limit > 0 {
        utils.Shuffle(l)
        return l[:limit]
    } else {
        var tl = make([]interface{}, 0)
        for i := 0; i < bits.AbsForInt32(limit); i ++ {
            if i >= len(l) {
                break
            }
            rand.NewSource(time.Now().UnixNano())
            ti := rand.Intn(len(l) - 1)
            tl = append(tl, l[ti])
        }
        return tl
    }
}

func (s *SetData) RemoveMutil(vs ...interface{}) int {
    s.check()
    c := 0
    for _, v := range vs {
        if s.data.Contains(v) {
            c += 1
        }
        s.data.Remove(v)
    }
    return c
}

func (s *SetData) AppendValue(interface{})                             {}
func (s *SetData) Incr(int64)                                          {}
func (s *SetData) GetSrcData() interface{}                             { return s.data }
func (s *SetData) SetValue(interface{})                                {}
func (s *SetData) GetValue() interface{}                               { return nil }
func (s *SetData) Slice(int, int)                                      {}
func (s *SetData) InsertValueByValue(int, interface{}, interface{})    {}
func (s *SetData) InsertValue(int, interface{})                        {}
func (s *SetData) PopValue(int) interface{}                            { return nil }
func (s *SetData) PopValueByValue(int, interface{}) int            { return -1 }
func (s *SetData) SetKeyValue(interface{}, interface{})                {}
func (s *SetData) SetIndexValue(int, interface{})                      {}
func (s *SetData) GetKeyValue(interface{}) interface{}                 { return nil }
func (s *SetData) GetIndexValue(int) interface{}                       { return nil }
func (s *SetData) HasKey(interface{}) bool                             { return false }
func (s *SetData) DelKey(interface{}) bool                             { return false }
func (s *SetData) GetKeys() []interface{}                              { return nil }

/////////////////////////////////////////////////////

func tryParseInt(v interface{}) int {
    s, ok := v.(string)
    if ok {
        n, err := strconv.Atoi(s)
        if err == nil {
            return n
        }
        return 0
    }
    return codecs.IntFromInterface(v)
}

func tryParseInt64(v interface{}) int64 {
    s, ok := v.(string)
    if ok {
        n, err := strconv.ParseInt(s, 10, 64)
        if err == nil {
            return n
        }
        return 0
    }
    return codecs.Int64FromInterface(v)
}

type LocalFastRedis struct {
    dataPool *sync.Map
}

func (r *LocalFastRedis) OpenConn(uint64) bool {
    return true
}

func (r *LocalFastRedis) CloseConn(uint64) {}

func (r *LocalFastRedis) InitPool(config RedisConfig) {
    r.dataPool = new(sync.Map)
}

func (r *LocalFastRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
    switch strings.ToLower(cmd) {
    case REDIS_COMMAND_GET:
        if len(args) == 1 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() == REDIS_TYPE_STANDARD {
                    return m.GetValue(), nil
                } else {
                    return nil, ErrorTypeNotMatch
                }
            }
            return nil, ErrorKeyNotFound
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SET:
        if len(args) == 2 {
            r.setData(args[0], args[1])
            return 1, nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_GETSET:
        if len(args) == 2 {
            oldData, _ := r.findData(args[0])
            r.setData(args[0], args[1])
            return oldData, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SETNX:
        if len(args) == 2 {
            if r.getData(args[0]) == nil {
                r.setData(args[0], args[1])
                return 1, nil
            } else {
                return 0, nil
            }
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_SETEX:
        if len(args) == 3 {
            r.setData(args[0], args[2])
            ex := tryParseInt64(args[1])
            if ex > 0 {
                r.setLifeCycle(args[0], time.Now().UnixNano() + ex)
            }
            return "OK", nil
        } else {
            return 0, ErrorArgsLength
        }

    case REDIS_COMMAND_STRLEN:
        if len(args) == 1 {
            pData, err := r.findData(args[0])
            if err == nil && pData != nil {
                strData, ok := pData.(string)
                if ok {
                    return len(strData), nil
                } else {
                    return 0, nil
                }
            }
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_INCR:
        if len(args) == 1 {
            return r.incrData(args[0], 1)
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_INCRBY:
        if len(args) == 2 {
            return r.incrData(args[0], tryParseInt64(args[1]))
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_DECR:
        if len(args) == 1 {
            return r.incrData(args[0], -1)
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_DECRBY:
        if len(args) == 2 {
            return r.incrData(args[0], 0 - tryParseInt64(args[1]))
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_APPEND:
        if len(args) == 2 {
            pData, err := r.findData(args[0])
            if err == nil && pData != nil {
                strData, ok := pData.(string)
                if ok {
                    exStrData, ok := args[1].(string)
                    if ok {
                        strData += exStrData
                    }
                    r.setData(args[0], strData)
                } else {
                    return nil, fmt.Errorf("no string data found for this key")
                }
            } else {
                r.setData(args[0], args[1])
                return r.findData(args[0])
            }
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_DEL:
        if len(args) == 1 {
            return r.delData(args[0]), nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_MGET:
        ret := make([]interface{}, 0)
        if len(args) > 0 {
            for _, v := range args {
                vv, _ := r.findData(v)
                ret = append(ret, vv)
            }

            return ret, nil
        } else {
            return ret, ErrorArgsLength
        }
    case REDIS_COMMAND_MSET:
        if len(args) >= 2 && len(args) % 2 == 0 {
            for i := 0; i < len(args) / 2; i ++ {
                k, v := args[i * 2], args[i * 2 + 1]
                r.setData(k, v)
            }
            return "OK", nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HEXISTS:
        if len(args) == 2 {
            m := r.getData(args[0])
            if m != nil && m.HasKey(args[1]) {
                return 1, nil
            }
            return 0, nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_HKEYS:
        ret := make([]interface{}, 0)
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                ret = m.GetKeys()
            }
            return ret, nil
        } else {
            return ret, ErrorArgsLength
        }
    case REDIS_COMMAND_HVALS:
        ret := make([]interface{}, 0)
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                ret = m.GetValues()
            }
            return ret, nil
        } else {
            return ret, ErrorArgsLength
        }
    case REDIS_COMMAND_HGET:
        if len(args) == 2 {
            return r.getMapData(args[0], args[1]), nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HGETALL:
        ret := make([]interface{}, 0)
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                keys := m.GetKeys()
                for _, key := range keys {
                    ret = append(ret, key)
                    ret = append(ret, m.GetKeyValue(key))
                }
            }
            return ret, nil
        } else {
            return ret, ErrorArgsLength
        }
    case REDIS_COMMAND_HSET:
        if len(args) == 3 {
            bExists := r.setMapData(args[0], args[1], args[2])
            if bExists {
                return 0, nil
            } else {
                return 1, nil
            }
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HSETNX:
        if len(args) == 3 {
            m := r.getData(args[0])
            if m == nil || m.GetDataType() == REDIS_TYPE_MAP {
                m = r.ensureMapData(args[0])
                if m.HasKey(args[1]) {
                    return 0, nil
                }
                m.SetKeyValue(args[1], args[2])
                return 1, nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HDEL:
        if len(args) >= 2 {
            ret := 0
            m := r.getData(args[0])
            if m != nil {
                for _, v := range args[1:] {
                    if m.DelKey(v) {
                        ret += 1
                    }
                }
            }
            return ret, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HMGET:
        if len(args) >= 2 {
            ret := make([]interface{}, 0)
            for _, v := range args[1:] {
                ret = append(ret, r.getMapData(args[0], v))
            }
            return ret, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HMSET:
        if len(args) >= 3 && len(args) % 2 == 1 {
            for i := 0; i < (len(args) - 1) / 2; i ++ {
                k, v := args[i * 2 + 1], args[i * 2 + 2]
                r.setMapData(args[0], k, v)
            }
            return "OK", nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_HLEN:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_MAP {
                    return nil, ErrorTypeNotMatch
                }
                return m.GetLength(), nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LLEN:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return 0, ErrorTypeNotMatch
                }
                return m.GetLength(), nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LPOP:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return nil, ErrorTypeNotMatch
                }
                return m.PopValue(0), nil
            }
            return nil, ErrorKeyNotFound
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_RPOP:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return nil, ErrorTypeNotMatch
                }
                return m.PopValue(-1), nil
            }
            return nil, ErrorKeyNotFound
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_LPUSH:
        if len(args) > 1 {
            m := r.ensureListData(args[0])
            if m != nil {
                m.InsertValue(0, args[1])
                return m.GetLength(), nil
            }
            return 0, ErrorTypeNotMatch
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_RPUSH:
        if len(args) > 1 {
            m := r.ensureListData(args[0])
            if m != nil {
                m.AppendValue(args[1])
                return m.GetLength(), nil
            }
            return 0, ErrorTypeNotMatch
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LSET:
        if len(args) > 2 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return "error", ErrorTypeNotMatch
                }
                at := tryParseInt(args[1])
                if at >= 0 && at < m.GetLength() {
                    m.SetIndexValue(at, args[2])
                    return "ok", nil
                }
                return "error", nil
            }
            return "error", ErrorKeyNotFound
        }
        return "fail", nil
    case REDIS_COMMAND_LINSERTAT:
        if len(args) > 2 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return 0, ErrorTypeNotMatch
                }
                at := tryParseInt(args[1])
                if at >= m.GetLength() {
                    at = -1
                }
                m.InsertValue(at, args[2])
                return m.GetLength(), nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LINSERT:
        if len(args) > 2 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return 0, ErrorTypeNotMatch
                }
                op, ok := args[1].(string)
                if !ok {
                    return 0, fmt.Errorf("invalid parameter")
                }
                rat := 0
                if op == "after" {
                    rat = 1
                }
                m.InsertValueByValue(rat, args[2], args[3])
                return m.GetLength(), nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LINDEX:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return nil, ErrorTypeNotMatch
                }
                at := tryParseInt(args[1])
                return m.GetIndexValue(at), nil
            }
            return nil, ErrorKeyNotFound
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_LREMAT:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return 0, ErrorTypeNotMatch
                }
                at := tryParseInt(args[1])
                if at >= m.GetLength() {
                    at = -1
                }
                m.PopValue(at)
                return 1, nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LREM:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return 0, ErrorTypeNotMatch
                }
                c := tryParseInt(args[1])
                return m.PopValueByValue(c, args[2]), nil
            }
            return 0, ErrorKeyNotFound
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_LRANGE:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return []interface{}{}, ErrorTypeNotMatch
                }
                vs := m.GetValues()
                s := 0
                e := len(vs) - 1
                if len(args) > 1 {
                    s = tryParseInt(args[1])
                }
                if len(args) > 2 {
                    e = tryParseInt(args[2])
                }
                if s < 0 {
                    s = 0
                }
                if e < 0 {
                    e = m.GetLength() + e
                }
                e = e + 1
                if e > len(vs) {
                    e = len(vs)
                }
                return vs[s:e], nil
            }
        }
        return []interface{}{}, ErrorKeyNotFound

    case REDIS_COMMAND_LTRIM:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                if m.GetDataType() != REDIS_TYPE_LIST {
                    return "error", ErrorTypeNotMatch
                }
                s := 0
                e := m.GetLength() - 1
                if len(args) > 1 {
                    s = tryParseInt(args[1])
                }
                if len(args) > 2 {
                    e = tryParseInt(args[2])
                }
                if s < 0 {
                    s = m.GetLength() + s
                }
                if e < 0 {
                    e = m.GetLength() + e
                }
                e = e + 1
                m.Slice(s, e)
                return "ok", nil
            }
        }
        return "error", ErrorKeyNotFound

    case REDIS_COMMAND_SADD:
        if len(args) > 1 {
            m := r.getData(args[0])
            c := 0
            if m != nil {
                for _, e := range args[1:] {
                    if m.Add(e) {
                        c += 1
                    }
                }
            }
            return c, nil
        } else {
            return 0, ErrorArgsLength
        }

    case REDIS_COMMAND_SCARD:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                return m.GetLength(), nil
            }
            return 0, nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_SDIFF:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                ml := make([]IPoolData, 0)
                for _, k := range args[1:] {
                    ms := r.getData(k)
                    if ms != nil {
                        ml = append(ml, ms)
                    }
                }
                return m.Diff(ml...), nil
            }

            return []interface{}{}, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SINTER:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                ml := make([]IPoolData, 0)
                for _, k := range args[1:] {
                    ms := r.getData(k)
                    if ms != nil {
                        ml = append(ml, ms)
                    }
                }
                return m.Inter(ml...), nil
            }

            return []interface{}{}, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SDIFFSTORE:
        if len(args) > 2 {
            m := r.getData(args[1])
            if m != nil {
                ml := make([]IPoolData, 0)
                for _, k := range args[2:] {
                    ms := r.getData(k)
                    if ms != nil {
                        ml = append(ml, ms)
                    }
                }
                r.initSetData(args[0], m.Diff(ml...))
            }
            c := 0
            mt := r.getData(args[0])
            if mt != nil {
               c = mt.GetLength()
            }
            return c, nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_SINTERSTORE:
        if len(args) > 2 {
            m := r.getData(args[1])
            if m != nil {
                ml := make([]IPoolData, 0)
                for _, k := range args[2:] {
                    ms := r.getData(k)
                    if ms != nil {
                        ml = append(ml, ms)
                    }
                }
                r.initSetData(args[0], m.Inter(ml...))
            }
            c := 0
            mt := r.getData(args[0])
            if mt != nil {
                c = mt.GetLength()
            }
            return c, nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_SISMEMBER:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                if m.Contains(args[1]) {
                    return 1, nil
                }
            }
            return 0, nil
        } else {
            return 0, ErrorArgsLength
        }
    case REDIS_COMMAND_SMEMBERS:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                return m.GetValues(), nil
            }
            return []interface{}{}, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SPOP:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                c := tryParseInt(args[1])
                if c == 0 {
                    c = 1
                }
                return m.PopValues(c), nil
            }
            return nil, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SRANDMEMBER:
        if len(args) > 0 {
            m := r.getData(args[0])
            if m != nil {
                c := tryParseInt(args[1])
                l := m.PeekValues(c)
                if c == 0 {
                    if len(l) > 0 {
                        return l[0], nil
                    } else {
                        return nil, nil
                    }
                } else {
                    return l, nil
                }
            }
            return nil, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SREM:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                return m.RemoveMutil(args[1:]), nil
            }
            return 0, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SUNION:
        if len(args) > 1 {
            m := r.getData(args[0])
            if m != nil {
                ml := make([]IPoolData, 0)
                for _, k := range args[1:] {
                    ms := r.getData(k)
                    if ms != nil {
                        ml = append(ml, ms)
                    }
                }
                return m.Union(ml...), nil
            }

            return []interface{}{}, nil
        } else {
            return nil, ErrorArgsLength
        }
    case REDIS_COMMAND_SUNIONSTORE:
        if len(args) > 2 {
            m := r.getData(args[1])
            if m != nil {
                ml := make([]IPoolData, 0)
                for _, k := range args[2:] {
                    ms := r.getData(k)
                    if ms != nil {
                        ml = append(ml, ms)
                    }
                }
                r.initSetData(args[0], m.Union(ml...))
            }
            c := 0
            mt := r.getData(args[0])
            if mt != nil {
                c = mt.GetLength()
            }
            return c, nil
        } else {
            return 0, ErrorArgsLength
        }
    }
    return nil, fmt.Errorf("command is not supported")
}

func (r *LocalFastRedis) Send(uint64, string, ...interface{}) error {
    return nil
}

func (r *LocalFastRedis) Flush(uint64) error {
    return nil
}

func (r *LocalFastRedis) Receive(uint64) (interface{}, error) {
    return nil, nil
}

func (r *LocalFastRedis) getData(k interface{}) IPoolData {
    id, ok := r.dataPool.Load(k)
    if ok {
        d, ok := id.(IPoolData)
        if ok {
            if d.CheckAlive() {
                return d
            } else {
                r.dataPool.Delete(k)
            }
        }
    }
    return nil
}

func (r *LocalFastRedis) delData(k interface{}) int {
    ret := 0
    _, ok := r.dataPool.Load(k)
    if !ok {
        ret = 1
    }
    r.dataPool.Delete(k)
    return ret
}

func (r *LocalFastRedis) ensureStandardData(k interface{}) IPoolData {
    var d IPoolData
    id, ok := r.dataPool.Load(k)
    if !ok {
        d = new(StandardData)
        r.dataPool.Store(k, d)
    } else {
        m, ok := id.(*StandardData)
        if ok {
            return m
        }
    }
    return d
}

func (r *LocalFastRedis) ensureMapData(k interface{}) IPoolData {
    var d IPoolData
    id, ok := r.dataPool.Load(k)
    if !ok {
        d = new(MapData)
        r.dataPool.Store(k, d)
    } else {
        m, ok := id.(*MapData)
        if ok {
            return m
        }
    }
    return d
}

func (r *LocalFastRedis) ensureListData(k interface{}) IPoolData {
    var d IPoolData
    id, ok := r.dataPool.Load(k)
    if !ok {
        d = new(ListData)
        r.dataPool.Store(k, d)
    } else {
        m, ok := id.(*ListData)
        if ok {
            return m
        }
    }
    return d
}

func (r *LocalFastRedis) ensureSetData(k interface{}) IPoolData {
    var d IPoolData
    id, ok := r.dataPool.Load(k)
    if !ok {
        d = new(SetData)
        r.dataPool.Store(k, d)
    } else {
        m, ok := id.(*SetData)
        if ok {
            return m
        }
    }
    return d
}

func (r *LocalFastRedis) incrData(k interface{}, add int64) (interface{}, error) {
    data := r.getData(k)
    var val interface{} = nil
    if data != nil {
        data.Incr(add)
        val = data.GetValue()
    }
    return val, nil
}

func (r *LocalFastRedis) findData(k interface{}) (interface{}, error) {
    data := r.getData(k)
    if data != nil {
        return data.GetValue(), nil
    } else {
        return nil, ErrorKeyNotFound
    }
}

func (r *LocalFastRedis) setLifeCycle(k interface{}, e int64) {
    data := r.getData(k)
    if data != nil {
        data.SetLifeCycle(e)
    }
}

func (r *LocalFastRedis) setData(k interface{}, d interface{}) {
    data := r.ensureStandardData(k)
    data.SetValue(d)
}

func (r *LocalFastRedis) setMapData(k interface{}, key interface{}, d interface{}) bool {
    data := r.ensureMapData(k)
    if data != nil {
        ret := data.HasKey(key)
        data.SetKeyValue(key, d)
        return ret
    }
    return false
}

func (r *LocalFastRedis) getMapData(k interface{}, key interface{}) interface{} {
    data := r.getData(k)
    if data != nil {
        return data.GetKeyValue(key)
    } else {
        return nil
    }
}

func (r *LocalFastRedis) initSetData(k interface{}, e ...interface{}) bool {
    data := r.ensureSetData(k)
    if data == nil {
        return false
    }
    data.BuildSet(e...)
    return true
}