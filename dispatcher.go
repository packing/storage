package main

import (
    "fmt"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/utils"
)

type StorageMessageObject struct {
}

func (receiver StorageMessageObject) OnQuery(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return messages.ErrorDataNotIsMessageMap
    }

    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(body)
    sql := r.StrValueOf(messages.ProtocolKeySQL, "")
    iArgs := r.TryReadValue(messages.ProtocolKeyArgs)
    args, ok := iArgs.(codecs.IMSlice)
    if sql == "" || !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    rows, err := mysqlClient.Query(sql, args...)
    if err == nil {
        srcData[messages.ProtocolKeyBody] = rows
    } else {
        srcData[messages.ProtocolKeyBody] = err.Error()
    }

    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnExec(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return messages.ErrorDataNotIsMessageMap
    }

    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(body)
    sql := r.StrValueOf(messages.ProtocolKeySQL, "")
    iArgs := r.TryReadValue(messages.ProtocolKeyArgs)
    args, ok := iArgs.(codecs.IMSlice)
    if sql == "" || !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    ret, err := mysqlClient.QueryWithoutResult(sql, args...)
    if err == nil {
        srcData[messages.ProtocolKeyBody] = ret
    } else {
        srcData[messages.ProtocolKeyBody] = err.Error()
    }

    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnTransaction(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return messages.ErrorDataNotIsMessageMap
    }

    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(body)
    iSqls := r.TryReadValue(messages.ProtocolKeySQL)
    sqls, ok := iSqls.(codecs.IMSlice)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    iArgs := r.TryReadValue(messages.ProtocolKeyArgs)
    args, ok := iArgs.(codecs.IMSlice)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    iActions := r.TryReadValue(messages.ProtocolKeyActions)
    actions, ok := iActions.(codecs.IMSlice)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }

    if len(sqls) != len(args) || len(sqls) != len(actions) {
        return messages.ErrorDataNotIsMessageMap
    }

    params := make([]TxParam, len(sqls))
    for i, iSql := range sqls {
        sql, ok := iSql.(string)
        if !ok {
            return messages.ErrorDataNotIsMessageMap
        }
        arg, ok := args[i].([]interface{})
        if !ok {
            return messages.ErrorDataNotIsMessageMap
        }
        action := codecs.IntFromInterface(actions[i])
        params[i] = TxParam{sql:sql, args:arg, action: action}
    }

    ret, err := mysqlClient.Transaction(params...)
    if err == nil {
        srcData[messages.ProtocolKeyBody] = ret
    } else {
        srcData[messages.ProtocolKeyBody] = err.Error()
    }

    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnRedisOpen(msg *messages.Message) error {
    //utils.LogError("OnRedisOpen")
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := r.UintValueOf(messages.ProtocolKeyKeyForRedis, 0)
    if key == 0 {
        return messages.ErrorDataNotIsMessageMap
    }
    if !redisClient.OpenConn(key) {
        srcData[messages.ProtocolKeyBody] = fmt.Errorf("cannot open the Redis connection").Error()
    } else {
        srcData[messages.ProtocolKeyBody] = true
    }
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnRedisClose(msg *messages.Message) error {
    //utils.LogError("OnRedisClose")
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := r.UintValueOf(messages.ProtocolKeyKeyForRedis, 0)
    if key == 0 {
        return messages.ErrorDataNotIsMessageMap
    }
    redisClient.CloseConn(key)
    srcData[messages.ProtocolKeyBody] = true
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnRedisDo(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return messages.ErrorDataNotIsMessageMap
    }

    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }

    r := codecs.CreateMapReader(body)

    cmd := r.StrValueOf(messages.ProtocolKeyCmd, "")
    iArgs := r.TryReadValue(messages.ProtocolKeyArgs)
    args, ok := iArgs.(codecs.IMSlice)
    if cmd == "" || !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    ret, e := redisClient.Do(cmd, args...)
    if e != nil {
        srcData[messages.ProtocolKeyBody] = e.Error()
    } else {
        m := make(codecs.IMMap)
        m[messages.ProtocolKeyResult] = ret
        srcData[messages.ProtocolKeyBody] = m
    }
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnRedisSend(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return messages.ErrorDataNotIsMessageMap
    }

    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := r.UintValueOf(messages.ProtocolKeyKeyForRedis, 0)
    if key == 0 {
        return messages.ErrorDataNotIsMessageMap
    }

    r = codecs.CreateMapReader(body)
    cmd := r.StrValueOf(messages.ProtocolKeyCmd, "")
    iArgs := r.TryReadValue(messages.ProtocolKeyArgs)
    args, ok := iArgs.(codecs.IMSlice)
    if cmd == "" || !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    e := redisClient.Send(key, cmd, args...)
    if e != nil {
        srcData[messages.ProtocolKeyBody] = e.Error()
    } else {
        srcData[messages.ProtocolKeyBody] = true
    }
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnRedisFlush(msg *messages.Message) error {
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := r.UintValueOf(messages.ProtocolKeyKeyForRedis, 0)
    if key == 0 {
        return messages.ErrorDataNotIsMessageMap
    }
    e := redisClient.Flush(key)
    if e != nil {
        srcData[messages.ProtocolKeyBody] = e.Error()
    } else {
        srcData[messages.ProtocolKeyBody] = true
    }
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnRedisReceive(msg *messages.Message) error {
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := r.UintValueOf(messages.ProtocolKeyKeyForRedis, 0)
    if key == 0 {
        return messages.ErrorDataNotIsMessageMap
    }
    ret, e := redisClient.Receive(key)
    if e != nil {
        srcData[messages.ProtocolKeyBody] = e.Error()
    } else {
        m := make(codecs.IMMap)
        m[messages.ProtocolKeyResult] = ret
        srcData[messages.ProtocolKeyBody] = m
    }
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnInitLock(msg *messages.Message) error {
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := uint64(r.IntValueOf(messages.ProtocolKeyKeyForLock, 0))

    //utils.LogInfo("Init Lock %d", key)

    keyLock.InitLock(key)

    //utils.LogInfo("Lock %d Inited", key)

    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnDisposeLock(msg *messages.Message) error {
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }
    r := codecs.CreateMapReader(srcData)
    key := uint64(r.IntValueOf(messages.ProtocolKeyKeyForLock, 0))

    //utils.LogInfo("Dispose Lock %d", key)

    keyLock.DisposeLock(key)

    //utils.LogInfo("Lock %d Disposed", key)

    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnLockKey(msg *messages.Message) error {
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }

    r := codecs.CreateMapReader(srcData)
    key := uint64(r.IntValueOf(messages.ProtocolKeyKeyForLock, 0))

    //utils.LogInfo("Lock %d", key)

    sid, err := keyLock.Lock(key)

    if err != nil {
        utils.LogInfo("Lock %d failure. Err: %s", key, err.Error())
    } else {
        //utils.LogInfo("Lock %d Success.", key)
    }

    srcData[messages.ProtocolKeyBody] = sid
    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) OnUnLockKey(msg *messages.Message) error {
    srcData, ok := msg.GetSrcData().(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }

    r := codecs.CreateMapReader(srcData)
    key := uint64(r.IntValueOf(messages.ProtocolKeyKeyForLock, 0))
    sid := r.IntValueOf(messages.ProtocolKeySidForLock, 0)

    //utils.LogInfo("UnLock %d - %d", key, sid)

    err := keyLock.Unlock(sid, key)

    if err != nil {
        utils.LogInfo("UnLock %d - %d failure. Err: %s", key, sid, err.Error())
    } else {
        //utils.LogInfo("UnLock %d - %d Success", key, sid)
    }

    if msg.GetUnixSource() != "" {
        unix.SendTo(msg.GetUnixSource(), srcData)
    } else {
        msg.GetController().Send(srcData)
    }
    return nil
}

func (receiver StorageMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeDBQuery] = receiver.OnQuery
    msgMap[messages.ProtocolTypeDBExec] = receiver.OnExec
    msgMap[messages.ProtocolTypeDBTransaction] = receiver.OnTransaction
    msgMap[messages.ProtocolTypeLockKey] = receiver.OnLockKey
    msgMap[messages.ProtocolTypeUnLockKey] = receiver.OnUnLockKey
    msgMap[messages.ProtocolTypeInitLockKey] = receiver.OnInitLock
    msgMap[messages.ProtocolTypeDisposeLockKey] = receiver.OnDisposeLock
    msgMap[messages.ProtocolTypeRedisOpen] = receiver.OnRedisOpen
    msgMap[messages.ProtocolTypeRedisClose] = receiver.OnRedisClose
    msgMap[messages.ProtocolTypeRedisDo] = receiver.OnRedisDo
    msgMap[messages.ProtocolTypeRedisSend] = receiver.OnRedisSend
    msgMap[messages.ProtocolTypeRedisFlush] = receiver.OnRedisFlush
    msgMap[messages.ProtocolTypeRedisReceive] = receiver.OnRedisReceive
    return msgMap
}
