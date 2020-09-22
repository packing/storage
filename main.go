package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "net/http"
    _ "net/http/pprof"
    "os"
    "syscall"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/env"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/nnet"
    "github.com/packing/nbpy/packets"
    "github.com/packing/nbpy/utils"
    "github.com/sipt/GoJsoner"
)

var (
    help    bool
    version bool

    daemon   bool
    setsided bool

    configfile = "./storage.conf"

    logDir   string
    logLevel = utils.LogLevelVerbose
    pidFile  string

    unix    *nnet.UnixUDP = nil
    tcp     *nnet.TCPServer = nil
)

func usage() {
    fmt.Fprint(os.Stderr, `storage

Usage: storage [-hv] [-d daemon] [-f config file]

Options:
`)
    flag.PrintDefaults()
}


func main() {

    flag.BoolVar(&help, "h", false, "help message")
    flag.BoolVar(&version, "v", false, "print version")
    flag.BoolVar(&daemon, "d", false, "run at daemon")
    flag.BoolVar(&setsided, "s", false, "already run at daemon")
    flag.StringVar(&configfile, "f", "./storage.conf", "config file")
    flag.Usage = usage

    flag.Parse()
    if help {
        flag.Usage()
        syscall.Exit(-1)
        return
    }
    if version {
        fmt.Println("storage version 1.0")
        syscall.Exit(-1)
        return
    }

    confContent, err := ioutil.ReadFile(configfile)
    if err != nil {
        utils.LogError("!!!读取配置文件 %s 失败", configfile, err)
        return
    }

    confString, err := GoJsoner.Discard(string(confContent))
    if err != nil {
        utils.LogError("!!!读取配置文件 %s 失败", configfile, err)
        return
    }

    globalConfig := Config{}
    err = json.Unmarshal([]byte(confString), &globalConfig)
    if err != nil {
        utils.LogError("!!!读取配置文件 %s 失败", configfile, err)
        return
    }

    logDir = globalConfig.LogDir
    if logDir == "" {
        logDir = "/dev/null"
    }
    logLevel = globalConfig.LogLevel
    if !daemon {
        logDir = ""
    } else {
        if !setsided {
            utils.Daemon()
            return
        }
    }

    pidFile = globalConfig.PIDFile
    if pidFile != "" {
        utils.GeneratePID(pidFile)
    }

    if globalConfig.PProfAddress != "" {
        go func() {
            http.ListenAndServe(globalConfig.PProfAddress, nil)
        }()
    }

    defer func() {
        if unix != nil {
            unix.Close()
            syscall.Unlink(globalConfig.UnixAddress)
        }

        if tcp != nil {
            tcp.Close()
        }

        if pidFile != "" {
            utils.RemovePID(pidFile)
        }

        utils.LogInfo(">>> 进程已退出")
    }()

    utils.LogInit(logLevel, logDir)

    //注册解码器
    env.RegisterCodec(codecs.CodecIMv2)

    //注册通信协议
    env.RegisterPacketFormat(packets.PacketFormatNB)

    //清理sock文件
    _, err = os.Stat(globalConfig.UnixAddress)
    if err == nil || !os.IsNotExist(err) {
        err = os.Remove(globalConfig.UnixAddress)
        if err != nil {
            utils.LogError("无法删除unix管道旧文件", err)
        }
    }

    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeS2S, messages.ProtocolTagStorage, StorageMessageObject{})
    messages.GlobalDispatcher.Dispatch()

    //初始化unixsocket发送管道
    unix = nnet.CreateUnixUDPWithFormat(packets.PacketFormatNB, codecs.CodecIMv2)
    unix.OnDataDecoded = messages.GlobalMessageQueue.Push
    err = unix.Bind(globalConfig.UnixAddress)
    if err != nil {
        utils.LogError("!!! 无法创建unixsocket管道 => %s", globalConfig.UnixAddress, err)
        unix.Close()
        return
    } else {
        utils.LogInfo("### 绑定 %s 成功", globalConfig.UnixAddress)
    }

    tcp = nnet.CreateTCPServer()
    tcp.OnDataDecoded = messages.GlobalMessageQueue.Push
    err = tcp.Bind(globalConfig.TCPAddress, 0)
    if err != nil {
        utils.LogError("!!! 无法在地址 %s 上开启监听", globalConfig.TCPAddress, err)
        unix.Close()
        tcp.Close()
        return
    }


    env.Schedule()

}

