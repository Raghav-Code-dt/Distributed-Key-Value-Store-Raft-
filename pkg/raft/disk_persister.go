package raft

import (
	"os"
	"sync"
)

// Storage defines the interface for persisting Raft state.
type Storage interface {
	SaveRaftState(state []byte)
	ReadRaftState() []byte
}

// DiskPersister saves Raft state to a physical file on disk.
type DiskPersister struct {
	mu       sync.Mutex
	filename string
}

// MakeDiskPersister creates a new DiskPersister.
func MakeDiskPersister(filename string) *DiskPersister {
	return &DiskPersister{
		filename: filename,
	}
}

// SaveRaftState atomically writes the state to the physical file.
func (dp *DiskPersister) SaveRaftState(state []byte) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	// Use os.WriteFile for a simple, atomic-ish write (in a real system, you'd use write-ahead logs or temp files and rename).
	err := os.WriteFile(dp.filename, state, 0644)
	if err != nil {
		panic("DiskPersister failed to write state to disk: " + err.Error())
	}
}

// ReadRaftState reads the state from the physical file.
func (dp *DiskPersister) ReadRaftState() []byte {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	data, err := os.ReadFile(dp.filename)
	if err != nil {
		// If the file doesn't exist, it's a fresh boot, return nil
		if os.IsNotExist(err) {
			return nil
		}
		panic("DiskPersister failed to read state from disk: " + err.Error())
	}
	return data
}
