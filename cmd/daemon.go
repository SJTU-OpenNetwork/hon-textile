package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	honlog "github.com/SJTU-OpenNetwork/hon-textile/hon-log"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/SJTU-OpenNetwork/hon-textile/api"
	"github.com/SJTU-OpenNetwork/hon-textile/bots"
	"github.com/SJTU-OpenNetwork/hon-textile/common"
	"github.com/SJTU-OpenNetwork/hon-textile/core"
	"github.com/SJTU-OpenNetwork/hon-textile/gateway"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
)

// Start the daemon against the user repository
func Daemon(repoPath string, pinCode string, docs bool, debug bool) error {
	var err error
	node, err = core.NewTextile(core.RunConfig{
		PinCode:  pinCode,
		RepoPath: repoPath,
		Debug:    debug,
	})
	if err != nil {
		return fmt.Errorf("create node failed: %s", err)
	}

	service := bots.NewService(node)
	enabledBots := make([]string, len(node.Config().Bots))
	for _, item := range node.Config().Bots {
		enabledBots = append(enabledBots, item.ID)
	}
	service.RunAll(repoPath, enabledBots)

	gateway.Host = &gateway.Gateway{
		Node: node,
		Bots: service,
	}

	api.Host = &api.Api{
		Node:     node,
		Bots:     service,
		PinCode:  pinCode,
		RepoPath: repoPath,
	}

	err = startNode(docs)
	if err != nil {
		return fmt.Errorf("start node failed: %s", err)
	}
	printSplash()

	// Shutdown gracefully if an SIGINT was received
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Interrupted")
	fmt.Printf("Shutting down...")
	err = stopNode()
	if err != nil && err != core.ErrStopped {
		fmt.Println(err.Error())
	} else {
		fmt.Print("done\n")
	}
	os.Exit(1)
	return nil
}

// Output the instance environment for the daemon command
func printSplash() {
	pid, err := node.PeerId()
	if err != nil {
		log.Fatalf("get peer id failed: %s", err)
	}
	fmt.Println(Grey("go-textile version: " + common.GitSummary))
	fmt.Println(Grey("Repo version: ") + Grey(repo.Repover))
	fmt.Println(Grey("Repo path: ") + Grey(node.RepoPath()))
	fmt.Println(Grey("API address: ") + Grey(api.Host.Addr()))
	fmt.Println(Grey("Gateway address: ") + Grey(gateway.Host.Addr()))
	if node.CafeApiAddr() != "" {
		fmt.Println(Grey("Cafe address: ") + Grey(node.CafeApiAddr()))
	}
	fmt.Println(Grey("System version: ") + Grey(runtime.GOARCH+"/"+runtime.GOOS))
	fmt.Println(Grey("Golang version: ") + Grey(runtime.Version()))
	fmt.Println(Grey("PeerID:  ") + Green(pid.Pretty()))
	fmt.Println(Grey("Account: ") + Cyan(node.Account().Address()))
}

type MetaAndNotification struct {
	notification *pb.Notification
	//feedStreamItem *pb.FeedItem
	feedItemPayload core.FeedItemPayload
}
type SafeMap struct {
	data map[string]*MetaAndNotification
	lock sync.Mutex
}

// Start the node, the API, and the Gateway
// And subsribe to updates of the wallet, thread, and notifications
func startNode(serveDocs bool) error {
	listener := node.ThreadUpdateListener()
	listener2 := node.Thread2UpdateListener()
	var metaNotiMap sync.Map
	//metaNotiMap := SafeMap{
	//	data : make(map[string]*MetaAndNotification),
	//	lock : sync.Mutex{},
	//}

	err := node.Start()
	if err != nil {
		return err
	}

	// subscribe to wallet updates
	go func() {
		for {
			select {
			case update, ok := <-node.UpdateCh():
				if !ok {
					return
				}
				switch update.Type {
				case pb.AccountUpdate_THREAD_ADDED:
					break
				case pb.AccountUpdate_THREAD_REMOVED:
					break
				case pb.AccountUpdate_ACCOUNT_PEER_ADDED:
					break
				case pb.AccountUpdate_ACCOUNT_PEER_REMOVED:
					break
				}
			}
		}
	}()

	// subscribe to thread updates
	go func() {
		for {
			select {
			case value, ok := <-listener.Ch:
				if !ok {
					return
				}
				if update, ok := value.(*pb.FeedItem); ok {
					if update == nil {
						log.Error("update is nil")
						continue
					}

					thrd := update.Thread[len(update.Thread)-8:]

					btype, err := core.FeedItemType(update)
					if err != nil {
						log.Error(err.Error())
						continue
					}
					log.Debugf("Get feed item %s", btype.String())

					payload, err := core.GetFeedItemPayload(update)
					if err != nil {
						log.Error(err.Error())
						continue
					}
					user := payload.GetUser()
					date := payload.GetDate()

					//Update metaNotiMap
					if meta, ok := payload.(*pb.FeedStreamMeta); ok {
						streamid := meta.Streammeta.Id
						fmt.Printf("cmdtest== update metanotimap,streamid: %s\n", streamid)
						//metaNotiMap.lock.Lock()
						if metaNoti, ok := metaNotiMap.Load(streamid); ok { //if metaNotiMap  include this stream
							//metaNot,_ := metaNotiMap.Load(streamid)
							//metaNoti := metaNot.(*MetaAndNotification)
							metaNotiMap.Store(streamid, &MetaAndNotification{feedItemPayload: payload, notification: metaNoti.(*MetaAndNotification).notification})
							fmt.Printf("cmdtest== metanotimap exist streamid, update metanotimap,feedstreamitemm: %s\n", update.String())
						} else {
							metaNotiMap.Store(streamid, &MetaAndNotification{feedItemPayload: payload, notification: nil})
							fmt.Printf("cmdtest== metanotimap doesn't exist streamid, update metanotimap,feedstreamitemm: %s\n", update.String())
						}
						//metaNotiMap.lock.Unlock()
					}

					// Subscribe automatically
					if node.Config().IsAuto && btype == pb.Block_STREAMMETA && user.Address != node.Profile().Address {
						log.Debug("[AUTO] Get Stream Meta")
						if meta, ok := payload.(*pb.FeedStreamMeta); ok {
							//StreamSubscribe(meta.Streammeta.Id)
							log.Debugf("[AUTO] Subscribe stream %s", meta.Streammeta.Id)
							err := node.SubscribeStream(meta.Streammeta.Id)
							if err != nil {
								log.Errorf("[AUTO] %v", err)
							}
						} else {
							log.Error("[AUTO] Can not convert FeedItemPayload to FeedStreamMeta")
						}
					}

					var txt string
					txt += time.Unix(0, util.ProtoNanos(date)).Format(time.RFC822)
					txt += "  "

					if user != nil {
						var name string
						if user.Name != "" {
							name = user.Name
						} else {
							if len(user.Address) >= 7 {
								name = user.Address[:7]
							} else {
								name = user.Address
							}
						}
						txt += name + " "
					}
					txt += "added "

					msg := Grey(txt) + Green(btype.String()) + Grey(" update to "+thrd)
					fmt.Println(msg)
				}
			}
		}
	}()

	// subscribe to notifications
	go func() {
		recordCache := util.NewSyncMap()
		for {
			select {
			case note, ok := <-node.NotificationCh():
				if !ok {
					return
				}

				// Automatically accept invite
				if note.Type == pb.Notification_INVITE_RECEIVED {
					log.Debug("[AUTO] New invite get")
					invites := node.Invites().Items
					for _, inv := range invites {
						hash, err := node.AcceptInvite(inv.Id)
						if err != nil {
							log.Errorf("[AUTO] Error when accept invite %v", err)
						} else {
							log.Debugf("[AUTO] Accept invite for %s", hash.String())
						}
					}
				}
				//=================
				// Show record
				if note.Type == pb.Notification_RECORD_REPORT && note.GetBody() != "" {
					showRecords(recordCache, note)
				}
				//=================
				if note.Type == pb.Notification_STREAM_FILE && note.GetBody() != "" {
					//fmt.Printf("cmdtest== notificationReceived "+note.String()+"\n")
					cid := note.GetBlock()
					sid := note.GetSubject()
					//metaNotiMap.lock.Lock()
					if metaNoti, ok := metaNotiMap.Load(sid); ok { //if metaNotiMap include this stream
						metaNotiMap.Store(sid, &MetaAndNotification{notification: note, feedItemPayload: metaNoti.(*MetaAndNotification).feedItemPayload})
						//fmt.Printf("cmdtest== metaNotiMap include this stream\n "+sid)
						//metaNotiMap.data[sid].notification = note
					} else {
						metaNotiMap.Store(sid, &MetaAndNotification{feedItemPayload: nil, notification: note})
						//fmt.Printf("cmdtest== metaNotiMap doesn't include this streamn\n "+sid)
					}
					//metaNotiMap.lock.Unlock()
					for { //wait for feedstreamitem
						//fmt.Printf("cmdtest== wait for feedstreamitem\n")
						metaNoti, _ := metaNotiMap.Load(sid)
						//metaNoti := metaNot.(*MetaAndNotification)
						if metaNoti.(*MetaAndNotification).feedItemPayload != nil {
							//fmt.Printf("cmdtest==show feeditem:%s\n,show noti: %s\n",
							//	metaNoti.(*MetaAndNotification).feedItemPayload.String(),
							//	metaNoti.(*MetaAndNotification).notification.String())
							//payload,err :=  core.GetFeedItemPayload(metaNoti.(*MetaAndNotification).feedStreamItem)//直接在map里放payload？？
							//if err != nil{

							//fmt.Printf("error when get feeditem payload\n")
							//fmt.Printf("cmdtest==print feeditem %s\n",payload.String())
							//log.Errorf("error when get feeditem payload: %s\n", err)
							//break
							//}
							if meta, ok := metaNoti.(*MetaAndNotification).feedItemPayload.(*pb.FeedStreamMeta); ok {
								//if meta,ok := payload.(*pb.FeedStreamMeta);ok {
								//StreamSubscribe(meta.Streammeta.Id)
								//log.Debugf("[AUTO] Subscribe stream %s", meta.Streammeta.Id)
								//err := node.SubscribeStream(meta.Streammeta.Id)
								//fmt.Printf("cmdtest== run storeFileCmd\n")
								storeFileCmd(cid, sid, meta)
								break
							} else {
								log.Error("[AUTO] Can not convert FeedItemPayload to FeedStreamMeta")
							}

						} else {
							//fmt.Printf("cmdtest== sleep 0.5s for feedstreamitem\n")
							time.Sleep(time.Millisecond * 500)
							//wait for feedstreamite update
						}
					}

				}
				//=================
				// Download files

				//=================
				date := util.ProtoTime(note.Date).Format(time.RFC822)
				var subject string
				if len(note.Subject) >= 7 {
					subject = note.Subject[len(note.Subject)-7:]
				}
				var msg string
				if note.User != nil {
					msg = Grey(date+"  "+note.User.Name+" ") + Cyan(note.Body) +
						Grey(" "+subject)
				} else {
					msg = Grey(date+"  ") + Grey(" "+subject)
				}
				fmt.Println(msg)
			}
		}
	}()

	// Subscribe to thread2 update:
	/*
		go func() {
			var err error
			<-node.OnlineCh()
			thread2Ch, err := node.Thread2Subscribe()
			if err != nil {
				log.Error("Error when subscribe thread2: ", err)
				fmt.Println("Error when subscribe thread2: ", err)
				return
			}
			var msg string
			var threadRecord *core.Thread2Record
			for record := range thread2Ch {
				threadRecord, err = node.UnmarshalRecord(record)
				if err != nil {
					log.Error("Error when unmarshal record: ", err)
					continue
				}
				msg = Green("Thread2 Record: "+"  "+threadRecord.ThreadId+" - "+threadRecord.LogId) + "\n" +
					Grey(string(threadRecord.Value))
				fmt.Println(msg)
			}
		}()
	*/
	//Subscribe to thread client update:
	// subscribe to thread2
	go func() {
		for {
			select {
			case value, ok := <-listener2.Ch:
				if !ok {
					return
				}
				fmt.Println("Received a update")
				if update, ok := value.(*core.Thread2UpdateMessage); ok {
					if update == nil {
						fmt.Println("update is nil")
						continue
					}
					instance := update.Event.Action.Instance

					fmt.Println("Received from thread2,value is ",instance)
				}

			}
		}
	}()

	//==================================
	// start apis
	api.Host.Start(node.Config().Addresses.API, serveDocs)
	gateway.Host.Start(node.Config().Addresses.Gateway)

	// start profiling api
	go func() {
		writeHeapDump("/debug/write-heap-dump/")
		freeOSMemory("/debug/free-os-memory/")
		mutexFractionOption("/debug/pprof-mutex/")
		err := http.ListenAndServe(node.Config().Addresses.Profiling, http.DefaultServeMux)
		if err != nil {
			log.Errorf("error starting profile listener: %s", err)
		}
	}()

	// Wait concurrently here until the node comes online
	// that is to say, until the online channel opens
	<-node.OnlineCh()

	// Textile is now online, continue
	return nil
}

func showRecords(store *util.SyncMap, n *pb.Notification) {
	//fmt.Printf("showRecord\n")
	var streamId string
	var ok bool
	block_map := make(map[string]string)
	err := json.Unmarshal([]byte(n.Block), &block_map)
	if err == nil {
		streamId, ok = block_map["ID"]
		if !ok {
			return
		}
	} else {
		streamId = n.Block
	}
	switch n.Subject {
	case recorder.Event_ThreadAddFile:
		fmt.Println("========================Show Record")
		store.Push(streamId, n.Date)
		// fmt.Printf("file 时间戳：%d,发送时刻：%d\n",n.Date.GetNanos()/1000000,time.Now())
		// fmt.Println(time.Now())
	case recorder.Event_DoneIPFSGet:
		// fmt.Printf("当前时间戳：%d，当前时刻：%d\n",ptypes.TimestampNow().GetNanos()/1000000,time.Now())
		// fmt.Println(time.Now())
		date := store.Get(streamId).(*timestamp.Timestamp)
		if date == nil {
			fmt.Printf("收到其他人发送的stream的反馈：%s\n", streamId)
		}
		// fmt.Printf("file 发送时间：%d;Noti 接收时间 %d\n",date.GetNanos()/1000000,n.Date.GetNanos()/1000000)
		duration := (n.Date.GetSeconds()-date.GetSeconds())*1000 + int64(n.Date.GetNanos()-date.GetNanos())/1000000
		fmt.Printf("发送用时统计：\n\tStream：\t%s\n\t接受者：\t%s\n\t总RTT：\t%dms\n",
			streamId, n.Actor, duration)
	}
}

// Stop the api, then the gateway, then the node, then if possible, the channels
// If a former fails, do not continue with the latter
func stopNode() error {
	err := api.Host.Stop()
	if err != nil {
		return err
	}
	err = gateway.Host.Stop()
	if err != nil {
		return err
	}
	err = node.Stop()
	if err != nil {
		return err
	}

	node.CloseChns()
	return nil
}

// mutexFractionOption allows to set runtime.SetMutexProfileFraction via HTTP
// using POST request with parameter 'fraction'.
func mutexFractionOption(path string) {
	http.DefaultServeMux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		err := r.ParseForm()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		asfr := r.Form.Get("fraction")
		if len(asfr) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		fr, err := strconv.Atoi(asfr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		log.Infof("Setting MutexProfileFraction to %d", fr)
		runtime.SetMutexProfileFraction(fr)
	})
}

// writeHeapDump writes a description of the heap and the objects in
// it to the given file descriptor. (used here for debugging)
func writeHeapDump(path string) {
	http.DefaultServeMux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		log.Infof("Writing heap dump")
		f, err := os.Create("heapdump")
		if err != nil {
			return
		}
		debug.WriteHeapDump(f.Fd())
	})
}

// freeOSMemory forces a garbage collection followed by an
// attempt to return as much memory to the operating system
// as possible. (used here for debugging)
func freeOSMemory(path string) {
	http.DefaultServeMux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		log.Infof("Freeing OS memory")
		debug.FreeOSMemory()
	})
}

// storeFileCmd store received files when get a Notification_STREAM_FILE
func storeFileCmd(cid string, streamid string, feedmeta *pb.FeedStreamMeta) {
	//create dir
	filespath := "/root/textile_testfiles/"
	_, err := os.Stat(filespath)
	if err != nil { //filepath doesn't exist
		if os.IsNotExist(err) {
			fmt.Println("dir is not exist,creating......")
			err := os.Mkdir(filespath, os.ModePerm)
			if err != nil {
				fmt.Printf("mkdir failed![%v]\n", err)
				return
			}
			fmt.Println("created dir")
		} else {
			fmt.Println("stat file error")
			return
		}
	}

	data, err := node.DataAtPath(cid)
	if err != nil {
		fmt.Printf("Error when call DataAtPath: " + err.Error())
	}

	file, err := os.OpenFile(
		filespath+cid,
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	duration := node.GetDuration(streamid)
	block_map := map[string]string{
		"ID":       streamid,
		"Parent":   node.StreamGetParent(streamid),
		"Duration": strconv.FormatInt(duration, 10),
	}
	block_json, err := json.Marshal(block_map)
	if err != nil {
		honlog.Hlog.Add("Error when marshal json" + err.Error())
		log.Error(err)
	}

	record := &pb.Notification{
		Block: streamid,
		Date:  ptypes.TimestampNow(),
		//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
		Subject: recorder.Event_DoneIPFSGet,
		Body:    string(block_json),
		Target:  feedmeta.PeerId,
		Read:    false, // Do not send to notification channel directly
	}
	recorder.RecordCh <- record
	//
}
