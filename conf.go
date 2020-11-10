package main

type MySQLConfig struct {
    Addr string `json:"addr"`
    DBName string `json:"dbName"`
    User string `json:"user"`
    Pwd string `json:"pwd"`
    PoolSize int `json:"pool"`
    IdleTime string `json:"idle"`
    LifeTime string `json:"life"`
    Encoding string `json:"encoding,omitempty"`
}

type RedisConfig struct {
    Addr string `json:"addr"`
    Pwd string `json:"pwd,omitempty"`
    Idle int `json:"maxIdle"`
    Active int `json:"maxActive"`
    IdleTime string `json:"idle"`
    LifeTime string `json:"life"`
}

type Config struct {
    MySQL MySQLConfig   `json:"mysql"`
    Redis RedisConfig   `json:"redis"`
    TCPAddress string   `json:"tcpAddr"`
    UnixAddress string  `json:"unixAddr"`
    LockLifeTime string `json:"lockLifeTime"`
    PIDFile string      `json:"pidFile,omitempty"`
    LogLevel int        `json:"logLevel,omitempty"`
    LogDir string       `json:"logDir,omitempty"`
    PProfAddress string `json:"pprof,omitempty"`
    LocalRedisInstance bool `json:"localRedisInstance"`
}