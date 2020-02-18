package chunk

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	. "github.com/claudetech/loggo/default"
)

// ErrTimeout is a timeout error
var ErrTimeout = errors.New("timeout")

// Storage is a chunk storage
type Storage struct {
	ChunkFile *os.File
	ChunkSize int64
	MaxChunks int
	chunks    map[string][]byte
	stack     *Stack
	lock      sync.Mutex
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, chunkFilePath string) (*Storage, error) {
	storage := Storage{
		ChunkSize: chunkSize,
		MaxChunks: maxChunks,
		chunks:    make(map[string][]byte),
		stack:     NewStack(maxChunks),
	}

	// Non-empty string in chunkFilePath enables MMAP disk storage for chunks
	if chunkFilePath != "" {
		chunkFile, err := os.OpenFile(chunkFilePath, os.O_RDWR|os.O_CREATE, 0600)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not open chunk cache file")
		}
		err = chunkFile.Truncate(chunkSize * int64(maxChunks))
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not resize chunk cache file")
		}
		Log.Infof("Created chunk cache file %v", chunkFile.Name())
		storage.ChunkFile = chunkFile
	}

	return &storage, nil
}

// newChunk creates a new mmap-backed chunk
func (s *Storage) newChunk() ([]byte, error) {
	if s.ChunkFile != nil {
		index := int64(s.stack.Len())
		Log.Debugf("Mmap chunk %v / %v", index+1, s.MaxChunks)
		chunk, err := syscall.Mmap(int(s.ChunkFile.Fd()), index*s.ChunkSize, int(s.ChunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}

		return chunk, nil
	} else {
		return make([]byte, s.ChunkSize), nil
	}
}

// Clear removes all old chunks on disk (will be called on each program start)
func (s *Storage) Clear() error {
	return nil
}

// Load a chunk from ram or creates it
func (s *Storage) Load(id string) []byte {
	s.lock.Lock()
	if chunk, exists := s.chunks[id]; exists {
		s.lock.Unlock()
		s.stack.Touch(id)
		return chunk
	}
	s.lock.Unlock()
	return nil
}

// Store stores a chunk in the RAM and adds it to the disk storage queue
func (s *Storage) Store(id string, bytes []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Avoid storing same chunk multiple times
	if _, exists := s.chunks[id]; exists {
		Log.Debugf("Create chunk %v (exists)", id)
		s.stack.Touch(id)
		return nil
	}

	deleteID := s.stack.Pop()
	if "" != deleteID {
		chunk := s.chunks[deleteID]
		delete(s.chunks, deleteID)
		Log.Debugf("Deleted chunk %v", deleteID)

		Log.Debugf("Create chunk %v (reused)", id)
		copy(chunk, bytes)
		s.chunks[id] = chunk
		s.stack.Push(id)
	} else {
		Log.Debugf("Create chunk %v (stored)", id)
		chunk, err := s.newChunk()
		if err != nil {
			return err
		}
		copy(chunk, bytes)
		s.chunks[id] = chunk
		s.stack.Push(id)
	}

	return nil
}
