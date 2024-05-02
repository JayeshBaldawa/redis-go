package storage

import (
	"sort"
	"sync"
)

type StreamEntry struct {
	ID         string
	Attributes map[string]interface{}
}

type StreamStorage struct {
	Stream          map[string][]StreamEntry
	IndexedEntryIDs sync.Map
}

var StreamStorageInstance *StreamStorage

func init() {
	StreamStorageInstance = &StreamStorage{
		Stream:          make(map[string][]StreamEntry),
		IndexedEntryIDs: sync.Map{},
	}
}

func (s *StreamStorage) GetStream(id string) []StreamEntry {
	return s.Stream[id]
}

func GetStreamStorage() *StreamStorage {
	return StreamStorageInstance
}

func (s *StreamStorage) AddEntry(EntryId string, attributes map[string]interface{}, StreamKey string) {
	entry := StreamEntry{
		ID:         EntryId,
		Attributes: attributes,
	}

	s.Stream[StreamKey] = append(s.Stream[StreamKey], entry)

	go s.IndexedEntryIDsStore(EntryId, StreamKey)
}

func (s *StreamStorage) IndexedEntryIDsStore(EntryId string, StreamKey string) {
	if val, ok := s.IndexedEntryIDs.Load(StreamKey); ok {
		ids := val.([]string)
		updatedIDs := append(ids, EntryId)
		sort.Strings(updatedIDs)
		s.IndexedEntryIDs.Store(StreamKey, updatedIDs)
	} else {
		s.IndexedEntryIDs.Store(StreamKey, []string{EntryId})
	}
}
