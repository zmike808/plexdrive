package chunk

import (
	"fmt"
	"syscall"

	. "github.com/claudetech/loggo/default"
	"github.com/dweidenfeld/plexdrive/drive"
)

// Manager manages chunks on disk
type Manager struct {
	ChunkSize  int64
	LoadAhead  int
	downloader *Downloader
	storage    *Storage
	queue      chan *queueEntry
	bufferPool *BufferPool
}

type queueEntry struct {
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
	Error  error
	Buffer *Buffer
}

// NewManager creates a new chunk manager
func NewManager(
	chunkSize int64,
	loadAhead,
	checkThreads int,
	loadThreads int,
	client *drive.Client,
	maxChunks int,
	maxPages int) (*Manager, error) {

	pageSize := int64(syscall.Getpagesize())
	if chunkSize < pageSize {
		return nil, fmt.Errorf("Chunk size must not be < %v", pageSize)
	}
	if chunkSize%pageSize != 0 {
		return nil, fmt.Errorf("Chunk size must be divideable by %v", pageSize)
	}
	if maxChunks < 2 || maxChunks < loadAhead {
		return nil, fmt.Errorf("max-chunks must be greater than 2 and bigger than the load ahead value")
	}
	if maxPages < 32 || maxPages > 256 {
		return nil, fmt.Errorf("Max pages must not be between 32 (FUSE default) and 256 (FUSE max)")
	}

	bufferPool := NewBufferPool(maxChunks+loadThreads, chunkSize)

	downloader, err := NewDownloader(loadThreads, client, bufferPool)
	if nil != err {
		return nil, err
	}

	manager := Manager{
		ChunkSize:  chunkSize,
		LoadAhead:  loadAhead,
		downloader: downloader,
		storage:    NewStorage(chunkSize, maxChunks),
		queue:      make(chan *queueEntry, 100),
		bufferPool: NewBufferPool(checkThreads+loadThreads, int64(maxPages)*pageSize),
	}

	if err := manager.storage.Clear(); nil != err {
		return nil, err
	}

	for i := 0; i < checkThreads; i++ {
		go manager.thread()
	}

	return &manager, nil
}

// GetChunk loads one chunk and starts the preload for the next chunks
func (m *Manager) GetChunk(object *drive.APIObject, offset, size int64) ([]byte, error) {
	buffer := m.bufferPool.Get()
	buffer.Ref()
	data := buffer.Bytes()[:size]

	// Handle unaligned requests across chunk boundaries (Direct-IO)
	for read := int64(0); read < size; {
		response := make(chan Response)

		m.requestChunk(object, offset+read, size-read, response)

		res := <-response
		if nil != res.Error {
			return nil, res.Error
		}

		bytes := adjustResponseChunk(offset+read, size-read, m.ChunkSize, res.Buffer.Bytes())
		read += int64(copy(data[read:], bytes))

		res.Buffer.Unref()
	}

	// Early unref, should be safe as long as bufferPool is properly sized.
	// We also know that the fuse package will copy the data when handling
	// the ReadResponse.
	buffer.Unref()
	return data, nil
}

func (m *Manager) requestChunk(object *drive.APIObject, offset, size int64, response chan Response) {
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

	m.queue <- &queueEntry{
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
			m.queue <- &queueEntry{
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
	if buffer := m.storage.Load(req.id); nil != buffer {
		if nil != response {
			buffer.Ref()
			response <- Response{
				Buffer: buffer,
			}
			close(response)
		}
		buffer.Unref()
		return
	}

	m.downloader.Download(req, func(err error, buffer *Buffer) {
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
			buffer.Ref()
			response <- Response{
				Buffer: buffer,
			}
			close(response)
		}

		if err := m.storage.Store(req.id, buffer); nil != err {
			Log.Warningf("Coult not store chunk %v", req.id)
		}
	})
}

func adjustResponseChunk(offset, size, chunkSize int64, bytes []byte) []byte {
	chunkOffset := offset % chunkSize
	chunkOffsetEnd := chunkOffset + size
	bytesLen := int64(len(bytes))

	sOffset := min(chunkOffset, bytesLen)
	eOffset := min(chunkOffsetEnd, bytesLen)

	return bytes[sOffset:eOffset]
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
