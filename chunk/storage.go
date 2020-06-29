package chunk

import (
	"errors"
	"sync"

	. "github.com/claudetech/loggo/default"
)

// ErrTimeout is a timeout error
var ErrTimeout = errors.New("timeout")

// Storage is a chunk storage
type Storage struct {
	ChunkSize  int64
	MaxChunks  int
	BufferPool *BufferPool
	chunks     map[string][]byte
	stack      *Stack
	lock       sync.RWMutex
}

// Item represents a chunk in RAM
type Item struct {
	id    string
	bytes []byte
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, bufferPool *BufferPool) *Storage {
	storage := Storage{
		ChunkSize:  chunkSize,
		MaxChunks:  maxChunks,
		BufferPool: bufferPool,
		chunks:     make(map[string][]byte, maxChunks),
		stack:      NewStack(maxChunks),
	}

	return &storage
}

// Clear removes all old chunks on disk (will be called on each program start)
func (s *Storage) Clear() error {
	return nil
}

// Load a chunk from ram or creates it
func (s *Storage) Load(id string) []byte {
	s.lock.RLock()
	if chunk, exists := s.chunks[id]; exists {
		s.stack.Touch(id)
		s.lock.RUnlock()
		return chunk
	}
	s.lock.RUnlock()
	return nil
}

// Store stores a chunk in the RAM and adds it to the disk storage queue
func (s *Storage) Store(id string, chunk []byte) error {
	s.lock.RLock()

	if _, exists := s.chunks[id]; exists {
		s.stack.Touch(id)
		s.lock.RUnlock()
		s.BufferPool.Put(chunk)
		return nil
	}

	s.lock.RUnlock()
	s.lock.Lock()

	deleteID := s.stack.Pop()
	if chunk, exists := s.chunks[deleteID]; exists {
		delete(s.chunks, deleteID)
		s.BufferPool.Put(chunk)

		Log.Debugf("Deleted chunk %v", deleteID)
	}

	s.chunks[id] = chunk
	s.stack.Push(id)
	s.lock.Unlock()

	return nil
}

// Get a buffer without exceeding max chunks
func (s *Storage) NewChunk(id string) []byte {
	s.lock.Lock()
	deleteID := s.stack.Pop()
	if chunk, exists := s.chunks[deleteID]; exists {
		delete(s.chunks, deleteID)
		Log.Debugf("Deleted chunk %v", deleteID)
		s.stack.Push(id)
		s.lock.Unlock()
		return chunk
	}
	s.stack.Push(id)
	s.lock.Unlock()
	return s.BufferPool.Get()
}

// Release a buffer from NewChunk without storing it
func (s *Storage) FreeChunk(id string, chunk []byte) {
	s.lock.Lock()
	s.stack.Purge(id)
	s.BufferPool.Put(chunk)
	s.lock.Unlock()
}
