package raftstore

/// A read only delegate of `Peer`.
type ReadDelegate struct {

}

type ReadProgress struct {

}

type ReadTaskType int

const (
	ReadTaskType_Register ReadTaskType = 0 + iota
	ReadTaskType_Update
	ReadTaskType_Read
	ReadTaskType_Destroy
)

type ReadTask struct {
	Type ReadTaskType
	RegionId uint64
	ReadDelegate *ReadDelegate
	ReadProgress *ReadProgress
	Message *Msg
}
