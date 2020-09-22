package main

type MySQLConfig struct {
    Addr string `json:"addr"`
    DBName string `json:"dbName"`
    User string `json:"user"`
    Pwd string `json:"pwd"`
    PoolSize int `json:"pool"`
    IdleTime string `json:"idle"`
    LifeTime string `json:"life"`
}

type RedisConfig struct {
    Addr string `json:"addr"`
    PoolSize int `json:"pool"`
    IdleTime int `json:"idle"`
}

type Config struct {
    MySQL MySQLConfig   `json:"mysql"`
    Redis RedisConfig   `json:"redis"`
    TCPAddress string   `json:"tcpAddr"`
    UnixAddress string  `json:"unixAddr"`
    PIDFile string      `json:"pidFile,omitempty"`
    LogLevel int        `json:"logLevel,omitempty"`
    LogDir string       `json:"logDir,omitempty"`
    PProfAddress string `json:"pprof,omitempty"`
}