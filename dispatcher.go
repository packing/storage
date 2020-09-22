package main

import "github.com/packing/nbpy/messages"

type StorageMessageObject struct {
}


func (receiver StorageMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
    return msgMap
}