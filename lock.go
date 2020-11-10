package main

import (
    "fmt"
    "sync"
    "sync/atomic"
    "time"
)

type LockChan struct {
    c chan int8
    s int64
}

type KeyLock struct {
    mutex sync.Mutex
    timeout time.Duration
    lcks map[uint64] *LockChan
}

func CreateKeyLock(timeout string) *KeyLock {
    t, err := time.ParseDuration(timeout)
    if err != nil {
        t = time.Minute * 10
    }
    kl := new(KeyLock)
    kl.lcks = make(map[uint64] *LockChan)
    kl.timeout = t
    return kl
}

func (l *KeyLock) InitLock(key uint64) {
    l.mutex.Lock()
    defer l.mutex.Unlock()

    lck := new(LockChan)
    lck.c = make(chan int8)
    lck.s = 0
    l.lcks[key] = lck
    go func() {
        lck.c <- 0
    }()
}

func (l *KeyLock) DisposeLock(key uint64) {
    l.mutex.Lock()
    defer l.mutex.Unlock()
    c, ok := l.lcks[key]
    if !ok {
        return
    }

    if c.s == 0 {
        <- c.c
    }
    close(c.c)
    delete(l.lcks, key)
}

func (l *KeyLock) getLck(key uint64) *LockChan {
    l.mutex.Lock()
    defer l.mutex.Unlock()
    c, ok := l.lcks[key]
    if !ok {
        return nil
    }
    return c
}

func (l *KeyLock) Lock(key uint64) (int64, error) {
    c := l.getLck(key)
    if c == nil {
        return 0, fmt.Errorf("lock %d need init", key)
    }
    _, ok := <- c.c
    if ok {
        atomic.StoreInt64(&c.s, time.Now().UnixNano())
        return c.s, nil
    }

    return c.s, fmt.Errorf("lock %d maybe is closed", key)
}

func (l *KeyLock) Unlock(s int64, key uint64) error {
    c := l.getLck(key)
    if c == nil {
        return fmt.Errorf("lock %d need init", key)
    }
    b := atomic.CompareAndSwapInt64(&c.s, s, 0)
    if !b {
        return fmt.Errorf("unlock %d maybe call by other", key)
    }
    go func() {
        c.c <- 0
    }()
    return nil
}