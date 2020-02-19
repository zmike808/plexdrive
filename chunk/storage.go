package chunk

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	. "github.com/claudetech/loggo/default"
)

// ErrTimeout is a timeout error
var ErrTimeout = errors.New("timeout")

// Storage is a chunk storage
type Storage struct {
	sync.RWMutex
	ChunkFile *os.File
	ChunkSize int64
	MaxChunks int
	chunks    map[string][]byte
	stack     *Stack
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

// Load a chunk from storage
func (s *Storage) Load(id string) *Chunk {
	s.RLock()
	if chunk, exists := s.chunks[id]; exists {
		s.stack.Touch(id)
		return NewChunk(chunk, s.RLocker())
	}
	s.RUnlock()
	return nil
}

// Copy contents of reader to a chunk in storage and return it
func (s *Storage) Store(id string, reader io.Reader) (*Chunk, error) {
	s.RLock()
	chunk, exists := s.chunks[id]

	// Avoid storing same chunk multiple times
	if exists {
		Log.Debugf("Create chunk %v (exists)", id)
		s.stack.Touch(id)
		return NewChunk(chunk, s.RLocker()), nil
	}

	s.RUnlock()
	s.Lock()

	var err error
	if deleteID := s.stack.Pop(); deleteID != "" {
		Log.Debugf("Create chunk %v (reused)", id)
		chunk = s.chunks[deleteID]

		delete(s.chunks, deleteID)
		Log.Debugf("Deleted chunk %v", deleteID)
	} else {
		Log.Debugf("Create chunk %v (stored)", id)
		chunk, err = s.newChunk()
		if err != nil {
			s.Unlock()
			return nil, err
		}
	}

	_, err = io.ReadFull(reader, chunk)
	s.Unlock()
	if nil != err && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	s.chunks[id] = chunk
	s.stack.Push(id)

	s.RLock()
	return NewChunk(chunk, s.RLocker()), nil
}
