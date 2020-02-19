package chunk

import "sync"

// Chunk holds a reference to a chunks bytes from storage and a read lock
type Chunk struct {
	Bytes []byte
	lock  sync.Locker
}

// NewChunk creates a new chunk
func NewChunk(bytes []byte, lock sync.Locker) *Chunk {
	return &Chunk{Bytes: bytes, lock: lock}
}

func (c *Chunk) Close() {
	c.lock.Unlock()
}
