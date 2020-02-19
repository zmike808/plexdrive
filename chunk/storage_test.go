package chunk

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/claudetech/loggo"
	. "github.com/claudetech/loggo/default"
)

func TestStore(t *testing.T) {
	chunkSize := int64(4 << 10) // 4K
	maxChunks := 2
	n := maxChunks * 2
	t.Run("Mem", func(t *testing.T) {
		useMmap := false
		testStore(chunkSize, maxChunks, useMmap, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
	t.Run("Disk", func(t *testing.T) {
		useMmap := true
		testStore(chunkSize, maxChunks, useMmap, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
}

func TestLoad(t *testing.T) {
	chunkSize := int64(4 << 10) // 4K
	maxChunks := 2
	n := maxChunks
	t.Run("Mem", func(t *testing.T) {
		useMmap := false
		testLoad(chunkSize, maxChunks, useMmap, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
	t.Run("Disk", func(t *testing.T) {
		useMmap := true
		testLoad(chunkSize, maxChunks, useMmap, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
}

func BenchmarkStore(b *testing.B) {
	chunkSize := int64(10 << 20) // 10M
	maxChunks := 32
	b.Run("Mem", func(b *testing.B) {
		useMmap := false
		testStore(chunkSize, maxChunks, useMmap, b.N, func(format string, args ...interface{}) {
			b.Fatalf(format, args...)
		})
	})
	b.Run("Disk", func(b *testing.B) {
		useMmap := true
		testStore(chunkSize, maxChunks, useMmap, b.N, func(format string, args ...interface{}) {
			b.Fatalf(format, args...)
		})
	})
}

func testStore(chunkSize int64, maxChunks int, useMmap bool, n int, errback func(string, ...interface{})) {
	// Silence logger
	logLevel := Log.Level()
	if logLevel != loggo.Fatal {
		Log.SetLevel(loggo.Fatal)
		defer Log.SetLevel(logLevel)
	}
	storage, err := newStorage(chunkSize, maxChunks, useMmap)
	if nil != err {
		errback("Failed to init storage: %v", err)
		return
	}
	defer storage.Close()
	data := make([]byte, chunkSize)
	buffer := bytes.NewReader(data)
	for i := 0; i < n; i++ {
		// Populate buffer with binary encoded value of i
		binary.LittleEndian.PutUint64(data, uint64(i))
		chunk, err := storage.Store(strconv.Itoa(i), buffer)
		if nil != err {
			errback("Failed to store buffer %v: %v", i, err)
			return
		}
		if j := int(binary.LittleEndian.Uint64(chunk.Bytes)); j != i {
			errback("Buffer mismatch – Expected %v got %v", i, j)
		}
		chunk.Close()
		buffer.Seek(0, 0)
	}
}

func testLoad(chunkSize int64, maxChunks int, useMmap bool, n int, errback func(string, ...interface{})) {
	// Silence logger
	logLevel := Log.Level()
	if logLevel != loggo.Fatal {
		Log.SetLevel(loggo.Fatal)
		defer Log.SetLevel(logLevel)
	}
	storage, err := newStorage(chunkSize, maxChunks, useMmap)
	if nil != err {
		errback("Failed to init storage: %v", err)
		return
	}
	defer storage.Close()
	data := make([]byte, chunkSize)
	buffer := bytes.NewReader(data)
	for i := 0; i < n; i++ {
		// Populate buffer with binary encoded value of i
		binary.LittleEndian.PutUint64(data, uint64(i))
		chunk, err := storage.Store(strconv.Itoa(i), buffer)
		if nil != err {
			errback("Failed to store buffer %v: %v", i, err)
			return
		}
		if j := int(binary.LittleEndian.Uint64(chunk.Bytes)); j != i {
			errback("Buffer mismatch – Expected %v got %v", i, j)
		}
		chunk.Close()
		buffer.Seek(0, 0)
	}
	for i := 0; i < n; i++ {
		// Populate buffer with binary encoded value of i
		binary.LittleEndian.PutUint64(data, uint64(i))
		chunk := storage.Load(strconv.Itoa(i))
		if nil == chunk {
			errback("Chunk %v not found", i)
			return
		} else {
			if j := int(binary.LittleEndian.Uint64(chunk.Bytes)); j != i {
				errback("Buffer mismatch – Expected %v got %v", i, j)
			}
			chunk.Close()
		}
	}
}

func newStorage(chunkSize int64, maxChunks int, useMmap bool) (*Storage, error) {
	var err error
	var chunkFile *os.File
	if useMmap {
		chunkFile, err = ioutil.TempFile("", "plexdrive-chunks-*.dat")
		if err != nil {
			return nil, err
		}
		os.Remove(chunkFile.Name())
	}
	return NewStorage(chunkSize, maxChunks, chunkFile)
}
