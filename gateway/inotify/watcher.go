package inotify

import (
	"bytes"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
	"wzinc/common"
	"wzinc/parser"
	"wzinc/rpc"

	"bytetrade.io/web3os/fs-lib/jfsnotify"

	"github.com/rs/zerolog/log"
)

var watcher *jfsnotify.Watcher

func WatchPath(path string) {
	// Create a new watcher.
	var err error
	watcher, err = jfsnotify.NewWatcher("myWatcher")
	if err != nil {
		panic(err)
	}

	// Start listening for events.
	go dedupLoop(watcher)
	log.Info().Msgf("watching path %s", path)

	err = filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			err = watcher.Add(path)
			if err != nil {
				fmt.Println("watcher add error:", err)
				return err
			}
		} else {
			err = updateOrInputDoc(path)
			if err != nil {
				log.Error().Msgf("udpate or input doc err %v", err)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func dedupLoop(w *jfsnotify.Watcher) {
	var (
		// Wait 1000ms for new events; each new event resets the timer.
		waitFor = 1000 * time.Millisecond

		// Keep track of the timers, as path → timer.
		mu           sync.Mutex
		timers       = make(map[string]*time.Timer)
		pendingEvent = make(map[string]jfsnotify.Event)

		// Callback we run.
		printEvent = func(e jfsnotify.Event) {
			log.Info().Msgf("handle event %v %v", e.Op.String(), e.Name)

			// Don't need to remove the timer if you don't have a lot of files.
			mu.Lock()
			delete(pendingEvent, e.Name)
			delete(timers, e.Name)
			mu.Unlock()
		}
	)

	for {
		select {
		// Read from Errors.
		case err, ok := <-w.Errors:
			if !ok { // Channel was closed (i.e. Watcher.Close() was called).
				return
			}
			printTime("ERROR: %s", err)
		// Read from Events.
		case e, ok := <-w.Events:
			if !ok { // Channel was closed (i.e. Watcher.Close() was called).
				log.Warn().Msg("watcher event channel closed")
				return
			}
			if e.Has(jfsnotify.Chmod) {
				continue
			}
			log.Debug().Msgf("pending event %v", e)
			// Get timer.
			mu.Lock()
			pendingEvent[e.Name] = e
			t, ok := timers[e.Name]
			mu.Unlock()

			// No timer yet, so create one.
			if !ok {
				t = time.AfterFunc(math.MaxInt64, func() {
					mu.Lock()
					ev := pendingEvent[e.Name]
					mu.Unlock()
					printEvent(ev)
					err := handleEvent(ev)
					if err != nil {
						log.Error().Msgf("handle watch file event error %s", err.Error())
					}
				})
				t.Stop()

				mu.Lock()
				timers[e.Name] = t
				mu.Unlock()
			}

			// Reset the timer for this path, so it will start from 100ms again.
			t.Reset(waitFor)
		}
	}
}

func handleEvent(e jfsnotify.Event) error {
	if e.Has(jfsnotify.Remove) || e.Has(jfsnotify.Rename) {
		log.Info().Msgf("push indexer task delete %s", e.Name)
		//VectorCli.fsTask <- VectorDBTask{
		//	Filename:  path.Base(e.Name),
		//	Filepath:  e.Name,
		//	IsInsert:  false,
		//	Action:    DeleteAction,
		//	TaskId:    uuid.NewString(),
		//	StartTime: time.Now().Unix(),
		//	FileId:    fileId(e.Name),
		//}
		res, err := rpc.RpcServer.EsQueryByPath(rpc.FileIndex, e.Name)
		if err != nil {
			return err
		}
		docs, err := rpc.EsGetFileQueryResult(res)
		if err != nil {
			return err
		}
		for _, doc := range docs {
			_, err = rpc.RpcServer.EsDelete(doc.DocId, rpc.FileIndex)
			if err != nil {
				log.Error().Msgf("zinc delete error %s", err.Error())
			}
			log.Debug().Msgf("delete doc id %s path %s", doc.DocId, e.Name)
		}
		//return nil
	}

	if e.Has(jfsnotify.Create) { // || e.Has(jfsnotify.Write) || e.Has(jfsnotify.Chmod) {
		err := filepath.Walk(e.Name, func(docPath string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				//add dir to watch list
				err = watcher.Add(docPath)
				if err != nil {
					log.Error().Msgf("watcher add error:%v", err)
				}
			} else {
				//input zinc file
				err = updateOrInputDoc(docPath)
				if err != nil {
					log.Error().Msgf("update or input doc error %v", err)
				}
			}
			return nil
		})
		if err != nil {
			log.Error().Msgf("handle create file error %v", err)
		}
		return nil
	}

	if e.Has(jfsnotify.Write) { // || e.Has(notify.Chmod) {
		return updateOrInputDoc(e.Name)
	}
	return nil
}

func updateOrInputDoc(filepath string) error {
	log.Debug().Msg("try update or input" + filepath)
	res, err := rpc.RpcServer.EsQueryByPath(rpc.FileIndex, filepath)
	if err != nil {
		return err
	}
	docs, err := rpc.EsGetFileQueryResult(res)
	if err != nil {
		return err
	}
	// path exist update doc
	if len(docs) > 0 {
		log.Debug().Msgf("has doc %v", docs[0].Where)
		//delete redundant docs
		if len(docs) > 1 {
			for _, doc := range docs[1:] {
				log.Debug().Msgf("delete redundant docid %s path %s", doc.DocId, doc.Where)
				_, err := rpc.RpcServer.EsDelete(doc.DocId, rpc.FileIndex)
				if err != nil {
					log.Error().Msgf("zinc delete error %v", err)
				}
			}
		}
		//update if doc changed
		f, err := os.Open(filepath)
		if err != nil {
			return err
		}
		b, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
		newMd5 := common.Md5File(bytes.NewReader(b))
		if newMd5 != docs[0].Md5 {
			//doc changed
			fileType := parser.GetTypeFromName(filepath)
			if _, ok := parser.ParseAble[fileType]; ok {
				log.Info().Msgf("push indexer task insert %s", filepath)
				//VectorCli.fsTask <- VectorDBTask{
				//	Filename:  path.Base(filepath),
				//	Filepath:  filepath,
				//	IsInsert:  true,
				//	Action:    AddAction,
				//	TaskId:    uuid.NewString(),
				//	StartTime: time.Now().Unix(),
				//	FileId:    fileId(filepath),
				//}
				content, err := parser.ParseDoc(bytes.NewReader(b), filepath)
				if err != nil {
					return err
				}
				log.Debug().Msgf("update content from old doc id %s path %s", docs[0].DocId, filepath)
				_, err = rpc.RpcServer.EsUpdateFileContentFromOldDoc(rpc.FileIndex, content, newMd5, docs[0])
				return err
			}
			log.Debug().Msgf("doc format not parsable %s", filepath)
			return nil
		}
		log.Debug().Msgf("ignore file %s md5: %s ", filepath, newMd5)
		return nil
	}

	log.Debug().Msgf("no history doc, add new")
	//path not exist input doc
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return err
	}
	md5 := common.Md5File(bytes.NewReader(b))
	fileType := parser.GetTypeFromName(filepath)
	content := ""
	if _, ok := parser.ParseAble[fileType]; ok {
		log.Info().Msgf("push indexer task insert %s", filepath)
		//VectorCli.fsTask <- VectorDBTask{
		//	Filename:  path.Base(filepath),
		//	Filepath:  filepath,
		//	IsInsert:  true,
		//	Action:    AddAction,
		//	TaskId:    uuid.NewString(),
		//	StartTime: time.Now().Unix(),
		//	FileId:    fileId(filepath),
		//}
		content, err = parser.ParseDoc(bytes.NewBuffer(b), filepath)
		if err != nil {
			return err
		}
	}
	filename := path.Base(filepath)
	size := 0
	fileInfo, err := os.Stat(filepath)
	if err == nil {
		size = int(fileInfo.Size())
	}
	doc := map[string]interface{}{
		"name":        filename,
		"where":       filepath,
		"md5":         md5,
		"content":     content,
		"size":        size,
		"created":     time.Now().Unix(),
		"updated":     time.Now().Unix(),
		"format_name": rpc.FormatFilename(filename),
	}
	id, err := rpc.RpcServer.EsInput(rpc.FileIndex, doc)
	log.Debug().Msgf("zinc input doc id %s path %s", id, filepath)
	return err
}

func printTime(s string, args ...interface{}) {
	log.Info().Msgf(time.Now().Format("15:04:05.0000")+" "+s+"\n", args...)
}
