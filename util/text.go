package util

import (
	"regexp"
	"sync"
	"fmt"
)

type callBack func(string) error
const (
	CMD_STREAM_META = "StreamMeta"
)
// ![cmd]/param1/param2/.../paramN
// Please note that there should also be a '/' if there is no parameter
// ![cmd]/
//var basicExpr = `!\[([\w]+)\]([/\w]+)`
//var basicExpr = `!\[([\w]+)\]([.\n]*)`
var basicExpr = `!\[([\w]+)\]([\s\S]*)`		// NOTE!!!! \n is not included in .

// Bot is used to handle special text message
type TextBot struct {
	//cmdRegs  map[string]*regexp.Regexp
	c_lock sync.Mutex
	basicReg *regexp.Regexp
	callBacks map[string] callBack
}

func (b *TextBot) Init() error {
	var err error
	b.basicReg, err = regexp.Compile(basicExpr)
	if err != nil {
		return err
	}
	b.callBacks = make(map[string] callBack)
	return err
}

func (b *TextBot) Register(cmd string, c callBack) {
	b.c_lock.Lock()
	defer b.c_lock.Unlock()
	b.callBacks[cmd] = c
}

func (b *TextBot) Deregister(cmd string) {
	b.c_lock.Lock()
	b.c_lock.Unlock()
	delete(b.callBacks, cmd)
}

func (b *TextBot) Execute(str string) (bool, error) {
	b.c_lock.Lock()
	defer b.c_lock.Unlock()
	params := b.basicReg.FindStringSubmatch(str)
	if len(params) > 2 {
		cmd := params[1]
		contains := params[2]
		cb, ok := b.callBacks[cmd]
		if !ok {
			fmt.Printf("No callback registered for %s\n", cmd)
			return false, nil
		} else {
			return true, cb(contains)
		}
	} else {
		// Not a command
		return false, nil
	}
}

// output: Whether is a string command, command, contains
func (b *TextBot) Extract(str string) (bool, string, string) {
	b.c_lock.Lock()
	defer b.c_lock.Unlock()
	params := b.basicReg.FindStringSubmatch(str)
	if len(params) > 2 {
		cmd := params[1]
		contains := params[2]
		return true, cmd, contains
	} else {
		// Not a command
		return false, "", ""
	}
}
