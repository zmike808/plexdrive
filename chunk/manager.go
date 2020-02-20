package chunk

import (
	"fmt"
	"math"
	"os"

	"github.com/dweidenfeld/plexdrive/drive"
)

// Manager manages chunks on disk
type Manager struct {
	ChunkSize  int64
	LoadAhead  int
	downloader *Downloader
	queue      chan *QueueEntry
}

type QueueEntry struct {
	request  *Request
	response chan Response
}

// Request represents a chunk request
type Request struct {
	id             string
	object         *drive.APIObject
	offsetStart    int64
	offsetEnd      int64
	chunkOffset    int64
	chunkOffsetEnd int64
	preload        bool
}

// Response represetns a chunk response
type Response struct {
	Error error
	Bytes []byte
}

// NewManager creates a new chunk manager
func NewManager(
	chunkFilePath string,
	chunkSize int64,
	loadAhead,
	checkThreads int,
	loadThreads int,
	client *drive.Client,
	maxChunks int) (*Manager, error) {

	var err error
	var chunkFile *os.File

	if chunkSize < 4096 {
		return nil, fmt.Errorf("Chunk size must not be < 4096")
	}
	if chunkSize%1024 != 0 {
		return nil, fmt.Errorf("Chunk size must be divideable by 1024")
	}
	if chunkFilePath != "" {
		pageSize := int64(os.Getpagesize())
		if chunkSize < pageSize {
			return nil, fmt.Errorf("Chunk size must not be < %v", pageSize)
		}
		if chunkSize%pageSize != 0 {
			return nil, fmt.Errorf("Chunk size must be divideable by %v", pageSize)
		}
		chunkFile, err = os.OpenFile(chunkFilePath, os.O_RDWR|os.O_CREATE, 0600)
		if nil != err {
			return nil, fmt.Errorf("Could not open chunk cache file %v: %v", chunkFilePath, err)
		}
	}
	if maxChunks < 2 || maxChunks < loadAhead {
		return nil, fmt.Errorf("max-chunks must be greater than 2 and bigger than the load ahead value")
	}

	storage, err := NewStorage(chunkSize, maxChunks, chunkFile)
	if nil != err {
		return nil, err
	}

	err = storage.Clear()
	if nil != err {
		return nil, err
	}

	downloader, err := NewDownloader(loadThreads, client, storage)
	if nil != err {
		return nil, err
	}

	manager := Manager{
		ChunkSize:  chunkSize,
		LoadAhead:  loadAhead,
		downloader: downloader,
		queue:      make(chan *QueueEntry, 100),
	}

	for i := 0; i < checkThreads; i++ {
		go manager.thread()
	}

	return &manager, nil
}

// GetChunk loads one chunk and starts the preload for the next chunks
func (m *Manager) GetChunk(object *drive.APIObject, offset, size int64, response chan Response) {
	chunkOffset := offset % m.ChunkSize
	offsetStart := offset - chunkOffset
	offsetEnd := offsetStart + m.ChunkSize
	id := fmt.Sprintf("%v:%v", object.ObjectID, offsetStart)

	request := &Request{
		id:             id,
		object:         object,
		offsetStart:    offsetStart,
		offsetEnd:      offsetEnd,
		chunkOffset:    chunkOffset,
		chunkOffsetEnd: chunkOffset + size,
		preload:        false,
	}

	m.queue <- &QueueEntry{
		request:  request,
		response: response,
	}

	for i := m.ChunkSize; i < (m.ChunkSize * int64(m.LoadAhead+1)); i += m.ChunkSize {
		aheadOffsetStart := offsetStart + i
		aheadOffsetEnd := aheadOffsetStart + m.ChunkSize
		if uint64(aheadOffsetStart) < object.Size && uint64(aheadOffsetEnd) < object.Size {
			id := fmt.Sprintf("%v:%v", object.ObjectID, aheadOffsetStart)
			request := &Request{
				id:          id,
				object:      object,
				offsetStart: aheadOffsetStart,
				offsetEnd:   aheadOffsetEnd,
				preload:     true,
			}
			m.queue <- &QueueEntry{
				request: request,
			}
		}
	}
}

func (m *Manager) thread() {
	for {
		queueEntry := <-m.queue
		m.checkChunk(queueEntry.request, queueEntry.response)
	}
}

func (m *Manager) checkChunk(req *Request, response chan Response) {
	m.downloader.Download(req, func(err error, bytes []byte) {
		if nil != err {
			if nil != response {
				response <- Response{
					Error: err,
				}
				close(response)
			}
			return
		}

		if nil != response {
			response <- Response{
				Bytes: adjustResponseChunk(req, bytes),
			}
			close(response)
		}
	})
}

func adjustResponseChunk(req *Request, bytes []byte) []byte {
	sOffset := int64(math.Min(float64(req.chunkOffset), float64(len(bytes))))
	eOffset := int64(math.Min(float64(req.chunkOffsetEnd), float64(len(bytes))))

	return bytes[sOffset:eOffset]
}
