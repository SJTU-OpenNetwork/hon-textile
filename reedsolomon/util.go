package reedsolomon

const intLength = 4
const byteMask = 255

func Int2Byte(i int) (b []byte) {
	b = make([]byte, intLength)
	b[0] = uint8(i & byteMask)
	b[1] = uint8(i >> 8 & byteMask)
	b[2] = uint8(i >> 16 & byteMask)
	b[3] = uint8(i >> 24 & byteMask)
	return b
}

func Byte2Int(b []byte) int {
	var i int = 0
	i |= (int(b[0]) & byteMask)
	i |= ((int(b[1]) << 8) & byteMask)
	i |= ((int(b[2]) << 16) & byteMask)
	i |= ((int(b[3]) << 24) & byteMask)
	return i
}

/*
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

 */