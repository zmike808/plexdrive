package chunk

import (
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
		storage, err := newStorage(chunkSize, maxChunks, useMmap)
		if nil != err {
			t.Fatal(err)
		}
		defer storage.Close()
		testStore(storage, chunkSize, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
	t.Run("Disk", func(t *testing.T) {
		useMmap := true
		storage, err := newStorage(chunkSize, maxChunks, useMmap)
		if nil != err {
			t.Fatal(err)
		}
		defer storage.Close()
		testStore(storage, chunkSize, n, func(format string, args ...interface{}) {
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
		storage, err := newStorage(chunkSize, maxChunks, useMmap)
		if nil != err {
			t.Fatal(err)
		}
		defer storage.Close()
		testStore(storage, chunkSize, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
		testLoad(storage, chunkSize, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
	t.Run("Disk", func(t *testing.T) {
		useMmap := true
		storage, err := newStorage(chunkSize, maxChunks, useMmap)
		if nil != err {
			t.Fatal(err)
		}
		defer storage.Close()
		testStore(storage, chunkSize, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
		testLoad(storage, chunkSize, n, func(format string, args ...interface{}) {
			t.Fatalf(format, args...)
		})
	})
}

func BenchmarkStore(b *testing.B) {
	chunkSize := int64(10 << 20) // 10M
	maxChunks := 10
	b.Run("Mem", func(b *testing.B) {
		useMmap := false
		storage, err := newStorage(chunkSize, maxChunks, useMmap)
		if nil != err {
			b.Fatal(err)
		}
		defer storage.Close()
		testStore(storage, chunkSize, b.N, func(format string, args ...interface{}) {
			b.Fatalf(format, args...)
		})
	})
	b.Run("Disk", func(b *testing.B) {
		useMmap := true
		storage, err := newStorage(chunkSize, maxChunks, useMmap)
		if nil != err {
			b.Fatal(err)
		}
		defer storage.Close()
		testStore(storage, chunkSize, b.N, func(format string, args ...interface{}) {
			b.Fatalf(format, args...)
		})
	})
}

func BenchmarkParallelStore(b *testing.B) {
	chunkSize := int64(10 << 20) // 10M
	maxChunks := 10
	useMmap := false
	memStorage, err := newStorage(chunkSize, maxChunks, useMmap)
	if nil != err {
		b.Fatal(err)
	}
	defer memStorage.Close()
	b.Run("Mem", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				testStore(memStorage, chunkSize, maxChunks, func(format string, args ...interface{}) {
					b.Fatalf(format, args...)
				})
			}
		})
	})
	useMmap = true
	diskStorage, err := newStorage(chunkSize, maxChunks, useMmap)
	if nil != err {
		b.Fatal(err)
	}
	defer diskStorage.Close()
	b.Run("Disk", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				testStore(diskStorage, chunkSize, maxChunks, func(format string, args ...interface{}) {
					b.Fatalf(format, args...)
				})
			}
		})
	})
}

func testStore(storage *Storage, chunkSize int64, n int, errback func(string, ...interface{})) {
	data := make([]byte, chunkSize)
	for i := 0; i < n; i++ {
		// Populate buffer with binary encoded value of i
		binary.LittleEndian.PutUint64(data, uint64(i))
		chunk, err := storage.Store(strconv.Itoa(i), data)
		if nil != err {
			errback("Failed to store buffer %v: %v", i, err)
			return
		}
		j := int(binary.LittleEndian.Uint64(chunk.Bytes))
		chunk.Close()
		if j != i {
			errback("Buffer mismatch – Expected %v got %v", i, j)
		}
	}
}

func testLoad(storage *Storage, chunkSize int64, n int, errback func(string, ...interface{})) {
	data := make([]byte, chunkSize)
	for i := 0; i < n; i++ {
		// Populate buffer with binary encoded value of i
		binary.LittleEndian.PutUint64(data, uint64(i))
		chunk := storage.Load(strconv.Itoa(i))
		if nil == chunk {
			errback("Chunk %v not found", i)
			return
		}
		j := int(binary.LittleEndian.Uint64(chunk.Bytes))
		chunk.Close()
		if j != i {
			errback("Buffer mismatch – Expected %v got %v", i, j)
		}
	}
}

func newStorage(chunkSize int64, maxChunks int, useMmap bool) (*Storage, error) {
	var err error
	var chunkFile *os.File
	// Silence logger
	Log.SetLevel(loggo.Fatal)
	Log.Infof("Initializing storage: chunkSize=%v, maxChunks=%v, useMmap=%v", chunkSize, maxChunks, useMmap)
	if useMmap {
		chunkFile, err = ioutil.TempFile("", "plexdrive-chunks-*.dat")
		if err != nil {
			return nil, err
		}
		os.Remove(chunkFile.Name())
	}
	return NewStorage(chunkSize, maxChunks, chunkFile)
}
