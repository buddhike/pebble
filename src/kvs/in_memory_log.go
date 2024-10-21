package kvs

import "github.com/buddhike/pebble/kvs/pb"

type inMemoryLog struct {
	entries []*pb.Entry
}

func (l *inMemoryLog) Append(entry *pb.Entry) {
	l.entries = append(l.entries, entry)
}

func (l *inMemoryLog) Get(idx int64) *pb.Entry {
	return l.entries[idx-1]
}

func (l *inMemoryLog) Truncate(after int64) {
	l.entries = l.entries[0:after]
}

func (l *inMemoryLog) Len() int64 {
	return int64(len(l.entries))
}

func (l *inMemoryLog) Last() *pb.Entry {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1]
	}
	return nil
}
