package raftstore

const (
	ErrStaleCommand = 1
)

type RaftStoreError struct {
	ErrType int
}

func NewStaleCommandErr() *RaftStoreError {
	return &RaftStoreError {
		ErrType: ErrStaleCommand,
	}
}
