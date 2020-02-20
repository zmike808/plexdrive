package chunk

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	. "github.com/claudetech/loggo/default"
	"github.com/dweidenfeld/plexdrive/drive"
)

// Downloader handles concurrent chunk downloads
type Downloader struct {
	Client    *drive.Client
	queue     chan *Request
	callbacks map[string][]DownloadCallback
	lock      sync.Mutex
	storage   *Storage
	buffers   map[int]bytes.Buffer
}

type DownloadCallback func(error, []byte)

// NewDownloader creates a new download manager
func NewDownloader(threads int, client *drive.Client, storage *Storage) (*Downloader, error) {
	manager := Downloader{
		Client:    client,
		queue:     make(chan *Request, 100),
		callbacks: make(map[string][]DownloadCallback, 100),
		storage:   storage,
		buffers:   make(map[int]bytes.Buffer, threads),
	}

	for i := 0; i < threads; i++ {
		go manager.thread(i)
	}

	return &manager, nil
}

// Download starts a new download request
func (d *Downloader) Download(req *Request, callback DownloadCallback) {
	// Serve from storage if possible
	if chunk := d.storage.Load(req.id); nil != chunk {
		callback(nil, chunk.Bytes)
		chunk.Close()
		return
	}
	d.lock.Lock()
	_, exists := d.callbacks[req.id]
	d.callbacks[req.id] = append(d.callbacks[req.id], callback)
	if !exists {
		d.queue <- req
	}
	d.lock.Unlock()
}

func (d *Downloader) thread(id int) {
	for {
		req := <-d.queue
		d.download(d.Client.GetNativeClient(), req, id)
	}
}

func (d *Downloader) download(client *http.Client, req *Request, threadId int) {
	Log.Debugf("Starting download %v (preload: %v)", req.id, req.preload)
	chunk, err := d.downloadFromAPI(client, req, 0, threadId)

	d.lock.Lock()
	callbacks := d.callbacks[req.id]
	for _, callback := range callbacks {
		callback(err, chunk.Bytes)
	}
	delete(d.callbacks, req.id)
	chunk.Close()
	d.lock.Unlock()
}

func (d *Downloader) downloadFromAPI(client *http.Client, request *Request, delay int64, threadId int) (*Chunk, error) {
	// sleep if request is throttled
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Second)
	}

	req, err := http.NewRequest("GET", request.object.DownloadURL, nil)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not create request object %v (%v) from API", request.object.ObjectID, request.object.Name)
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%v-%v", request.offsetStart, request.offsetEnd))

	Log.Tracef("Sending HTTP Request %v", req)

	res, err := client.Do(req)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not request object %v (%v) from API", request.object.ObjectID, request.object.Name)
	}
	defer res.Body.Close()
	reader := res.Body

	if res.StatusCode != 206 {
		if res.StatusCode != 403 && res.StatusCode != 500 {
			Log.Debugf("Request\n----------\n%v\n----------\n", req)
			Log.Debugf("Response\n----------\n%v\n----------\n", res)
			return nil, fmt.Errorf("Wrong status code %v for %v", res.StatusCode, request.object)
		}

		// throttle requests
		if delay > 8 {
			return nil, fmt.Errorf("Maximum throttle interval has been reached")
		}
		bytes, err := ioutil.ReadAll(reader)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not read body of error")
		}
		body := string(bytes)
		if strings.Contains(body, "dailyLimitExceeded") ||
			strings.Contains(body, "userRateLimitExceeded") ||
			strings.Contains(body, "rateLimitExceeded") ||
			strings.Contains(body, "backendError") ||
			strings.Contains(body, "internalError") {
			if 0 == delay {
				delay = 1
			} else {
				delay = delay * 2
			}
			return d.downloadFromAPI(client, request, delay, threadId)
		}

		// return an error if other error occurred
		Log.Debugf("%v", body)
		return nil, fmt.Errorf("Could not read object %v (%v) / StatusCode: %v",
			request.object.ObjectID, request.object.Name, res.StatusCode)
	}

	buffer := d.buffers[threadId]
	buffer.Reset()
	if _, err = buffer.ReadFrom(reader); nil != err {
		Log.Debug(err)
		return nil, fmt.Errorf("Could not buffer object %v (%v) API response", request.object.ObjectID, request.object.Name)
	}

	chunk, err := d.storage.Store(request.id, buffer.Bytes())
	if nil != err {
		Log.Debug(err)
		return nil, fmt.Errorf("Could not store object %v (%v)", request.object.ObjectID, request.object.Name)
	}

	return chunk, nil
}
