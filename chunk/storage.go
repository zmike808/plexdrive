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
	sync.RWMutex
	ChunkFile *os.File
	ChunkSize int64
	MaxChunks int
	chunks    map[string]*Chunk
	stack     *Stack
	numChunks int
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, chunkFile *os.File) (*Storage, error) {
	storage := Storage{
		ChunkSize: chunkSize,
		MaxChunks: maxChunks,
		chunks:    make(map[string]*Chunk, maxChunks),
		stack:     NewStack(maxChunks),
	}

	// Non-empty string in chunkFilePath enables MMAP disk storage for chunks
	if chunkFile != nil {
		err := chunkFile.Truncate(chunkSize * int64(maxChunks))
		if nil != err {
			return nil, fmt.Errorf("Could not resize chunk cache file %v: %v", chunkFile.Name(), err)
		}
		Log.Infof("Created chunk cache file %v", chunkFile.Name())
		storage.ChunkFile = chunkFile
	}

	return &storage, nil
}

// newChunk creates a new mmap-backed chunk
func (s *Storage) newChunk() (*Chunk, error) {
	if s.numChunks >= s.MaxChunks {
		return nil, fmt.Errorf("Tried to allocate chunk %v / %v", s.numChunks+1, s.MaxChunks)
	}
	Log.Debugf("Allocate chunk %v / %v", s.numChunks+1, s.MaxChunks)
	if s.ChunkFile != nil {
		bytes, err := syscall.Mmap(int(s.ChunkFile.Fd()), int64(s.numChunks)*s.ChunkSize, int(s.ChunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}
		s.numChunks++
		return NewChunk(bytes, s.RLocker()), nil
	} else {
		s.numChunks++
		return NewChunk(make([]byte, s.ChunkSize), s.RLocker()), nil
	}
}

// Clear removes all old chunks on disk (will be called on each program start)
func (s *Storage) Clear() error {
	return nil
}

// Close open file descriptors
func (s *Storage) Close() error {
	s.Lock()
	defer s.Unlock()
	if nil != s.ChunkFile {
		return nil
	}
	return s.ChunkFile.Close()
}

// Load a chunk from storage
func (s *Storage) Load(id string) *Chunk {
	s.RLock()
	chunk, exists := s.chunks[id]
	if exists {
		s.stack.Touch(id)
		return chunk
	}
	s.RUnlock()
	return nil
}

// Copy contents of reader to a chunk in storage and return it
func (s *Storage) Store(id string, bytes []byte) (*Chunk, error) {
	s.RLock()
	chunk, exists := s.chunks[id]

	// Avoid storing same chunk multiple times
	if exists {
		Log.Debugf("Create chunk %v (exists)", id)
		s.stack.Touch(id)
		return chunk, nil
	}

	s.RUnlock()
	s.Lock()

	var err error
	// Log.Infof("Stack len %v", s.stack.Len())
	if deleteID := s.stack.Pop(); deleteID != "" {
		Log.Debugf("Create chunk %v (reused %v)", id, deleteID)
		chunk = s.chunks[deleteID]
		delete(s.chunks, deleteID)
		Log.Debugf("Deleted chunk %v", deleteID)
	} else {
		Log.Debugf("Create chunk %v (stored)", id)
		chunk, err = s.newChunk()
		if err != nil {
			Log.Debugf("newChunk %v failed", id)
			s.Unlock()
			return nil, err
		}
	}

	copy(chunk.Bytes, bytes)
	s.chunks[id] = chunk
	s.stack.Push(id)
	s.Unlock()

	s.RLock()
	return chunk, nil
}
