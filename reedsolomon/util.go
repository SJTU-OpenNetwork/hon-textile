package reedsolomon

import "unsafe"

func Int2Byte(data int)(ret []byte){
	var len uintptr = unsafe.Sizeof(data)
	ret = make([]byte, len)
	var tmp int = 0xff
	var index uint = 0
	for index=0; index<uint(len); index++{
		ret[index] = byte((tmp<<(index*8) & data)>>(index*8))
	}
	return ret
}

func Byte2Int(data []byte)int{
	var ret int = 0
	var len int = len(data)
	var i uint = 0
	for i=0; i<uint(len); i++{
		ret = ret | (int(data[i]) << (i*8))
	}
	return ret
}