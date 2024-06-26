package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	events "github.com/codecrafters-io/redis-starter-go/app/events"
	log "github.com/codecrafters-io/redis-starter-go/app/logger"
	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
)

type StreamEntry struct {
	ID         string
	Attributes map[string]interface{}
}

type StreamStorage struct {
	Stream          map[string]map[string]StreamEntry
	IndexedEntryIDs sync.Map
	IncrementRWLock sync.RWMutex
}

var StreamStorageInstance *StreamStorage

func init() {
	StreamStorageInstance = &StreamStorage{
		Stream:          make(map[string]map[string]StreamEntry),
		IndexedEntryIDs: sync.Map{},
		IncrementRWLock: sync.RWMutex{},
	}
}

func GetStreamStorage() *StreamStorage {
	return StreamStorageInstance
}

func (s *StreamStorage) AddEntry(EntryId string, attributes map[string]interface{}, StreamKey string) (string, error) {

	newEntryId, err := s.IndexedEntryIDsStore(EntryId, StreamKey)

	if err != nil {
		return "", err
	}

	entry := StreamEntry{
		ID:         newEntryId,
		Attributes: attributes,
	}

	if _, ok := s.Stream[StreamKey]; !ok {
		s.Stream[StreamKey] = make(map[string]StreamEntry)
	}

	s.Stream[StreamKey][newEntryId] = entry

	// NewEvent
	eventData := events.Event{
		Topic: parserModel.XREAD_TOPIC + "_" + StreamKey,
		Data:  entry,
	}

	// Publish the new entry to the subscribers
	events.GetPubSub().Publish(eventData)

	return newEntryId, nil
}

func (s *StreamStorage) IndexedEntryIDsStore(newEntryID string, streamKey string) (string, error) {

	if newEntryID == "*" {
		newEntryID = s.GenerateStreamEntryID()
		return newEntryID, nil
	}

	parts := strings.Split(newEntryID, "-")

	// Validate the newEntryID format
	timestamp := parts[0]
	sequence := parts[1]

	if sequence == "*" {
		newEntryID = s.IncrementSequenceNumber(streamKey, timestamp)
		// Split the newEntryID to get the timestamp and sequence
		parts := strings.Split(newEntryID, "-")
		timestamp = parts[0]
		sequence = parts[1]
	}

	timestampInt, err := strconv.Atoi(timestamp)
	if err != nil {
		return "", errors.New("invalid timestamp in entry ID")
	}

	sequenceInt, err := strconv.Atoi(sequence)
	if err != nil {
		return "", errors.New("invalid sequence number in entry ID")
	}

	// Check if the newEntryID is valid based on the stream's existing entries
	if err := s.validateNewEntryID(streamKey, timestampInt, sequenceInt); err != nil {
		return "", err
	}

	// If the newEntryID is valid, add it to the IndexedEntryIDs
	entries, ok := s.IndexedEntryIDs.Load(streamKey)
	if !ok {
		s.IndexedEntryIDs.Store(streamKey, []string{newEntryID})
		return newEntryID, nil
	}

	entries = append(entries.([]string), newEntryID)
	s.IndexedEntryIDs.Store(streamKey, entries)

	return newEntryID, nil
}

// validateNewEntryID checks if the newEntryID is valid based on the existing stream entries.
func (s *StreamStorage) validateNewEntryID(streamKey string, newTimestamp int, newSequence int) error {

	lastEntryID := s.GetLastEntryID(streamKey)

	if lastEntryID == "" {
		lastEntryID = "0-0"
	}

	lastParts := strings.Split(lastEntryID, "-")
	lastTimestamp, err := strconv.Atoi(lastParts[0])
	if err != nil {
		return errors.New("invalid timestamp in existing entry ID")
	}

	lastSequence, err := strconv.Atoi(lastParts[1])
	if err != nil {
		return errors.New("invalid sequence number in existing entry ID")
	}

	if newTimestamp == 0 && newSequence == 0 {
		return errors.New("The ID specified in XADD must be greater than 0-0")
	}

	// Check if the newEntryID's timestamp and sequence are greater than the last entry
	if newTimestamp < lastTimestamp || (newTimestamp == lastTimestamp && newSequence <= lastSequence) {
		return errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

// IncrementSequenceNumber increments the sequence number of the last entry in the stream.
func (s *StreamStorage) IncrementSequenceNumber(streamKey string, timestamp string) string {
	// Aquire the lock
	s.IncrementRWLock.Lock()
	defer s.IncrementRWLock.Unlock()

	entries, ok := s.IndexedEntryIDs.Load(streamKey)
	if !ok {
		var newEntryID string
		if timestamp == "0" {
			newEntryID = "0-1"
		} else {
			newEntryID = timestamp + "-0"
		}
		return newEntryID
	}

	entryIDs := entries.([]string)
	lastEntryID := timestamp + "-0"
	foundLastEntry := false

	// Get the last entry ID which has the timestamp same -- reverse loop
	for i := len(entryIDs) - 1; i >= 0; i-- {
		if strings.HasPrefix(entryIDs[i], timestamp) {
			lastEntryID = entryIDs[i]
			foundLastEntry = true
			break
		}
	}

	parts := strings.Split(lastEntryID, "-")
	if len(parts) != 2 {
		return ""
	}

	sequence, err := strconv.Atoi(parts[1])
	if err != nil {
		return ""
	}

	if !foundLastEntry {
		sequence = 0
	} else {
		sequence++
	}

	newEntryID := parts[0] + "-" + strconv.Itoa(sequence)

	return newEntryID
}

func (s *StreamStorage) GenerateStreamEntryID() string {
	timestamp := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	return timestamp + "-0"
}

func (s *StreamStorage) GetStream(id string) map[string]StreamEntry {
	return s.Stream[id]
}

func (s *StreamStorage) GetRange(keyName string, start string, end string) []StreamEntry {

	anyEntries, ok := s.IndexedEntryIDs.Load(keyName)

	if !ok {
		return nil
	}

	entries := anyEntries.([]string)

	var (
		startTimestamp string
		startSequence  string = "0"
		endTimestamp   string
		endSequence    string = "0"
	)

	streamEntries, ok := s.Stream[keyName]
	if !ok {
		return nil
	}

	if start == "-" || start == "" {
		start = "0-0"
	}

	if end == "+" || end == "" {
		end = entries[len(entries)-1]
	}

	if strings.Contains(start, "-") {
		startParts := strings.Split(start, "-")
		startTimestamp = startParts[0]
		startSequence = startParts[1]
	} else {
		startTimestamp = start
	}

	if strings.Contains(end, "-") {
		endParts := strings.Split(end, "-")
		endTimestamp = endParts[0]
		endSequence = endParts[1]
	} else {
		endTimestamp = end
	}

	var StreamEntryList []StreamEntry

	endTimestampInt, err := strconv.Atoi(endTimestamp)
	if err != nil {
		log.LogError(err)
		return nil
	}

	endSequenceInt, err := strconv.Atoi(endSequence)
	if err != nil {
		log.LogError(err)
		return nil
	}

	startSequenceInt, err := strconv.Atoi(startSequence)
	if err != nil {
		log.LogError(err)
		return nil
	}

	startTimestampInt, err := strconv.Atoi(startTimestamp)
	if err != nil {
		log.LogError(err)
		return nil
	}

	for _, entry := range entries {

		// Check if the entry is within the range
		entryParts := strings.Split(entry, "-")
		entryTimestampInt, err := strconv.Atoi(entryParts[0])
		if err != nil {
			log.LogError(err)
			return nil
		}

		entrySequenceInt, err := strconv.Atoi(entryParts[1])
		if err != nil {
			log.LogError(err)
			return nil
		}

		// Case 1: If the entry's timestamp is greater than the end timestamp
		if entryTimestampInt > endTimestampInt {
			break
		}

		// Case 2: If the entry's timestamp is equal to the end timestamp but the sequence is greater than the end sequence
		if entryTimestampInt == endTimestampInt && entrySequenceInt > endSequenceInt {
			break
		}

		// Case 3: If the entry's timestamp is equal to the start timestamp but the sequence is less than the start sequence
		if entryTimestampInt == startTimestampInt && entrySequenceInt < startSequenceInt {
			continue
		}

		// Case 4: If the entry's timestamp is less than the start timestamp
		if entryTimestampInt < startTimestampInt {
			continue
		}

		StreamEntryList = append(StreamEntryList, streamEntries[entry])
	}

	return StreamEntryList
}

func (s *StreamStorage) GetLastEntryID(streamKey string) string {
	// Aquire the lock
	s.IncrementRWLock.RLock()
	defer s.IncrementRWLock.RUnlock()

	entries, ok := s.IndexedEntryIDs.Load(streamKey)
	if !ok {
		return ""
	}

	entryIDs := entries.([]string)
	return entryIDs[len(entryIDs)-1]
}

func (s *StreamStorage) XReadStreams(xReadEntries map[string]string) map[string][]StreamEntry {

	var entries map[string][]StreamEntry = make(map[string][]StreamEntry)

	for streamKeyName, entryID := range xReadEntries {

		entries[streamKeyName] = s.GetRange(streamKeyName, entryID, "+")
	}

	return entries
}

func isEntryInRange(newEntryId string, existingEntryId string) (bool, error) {
	newParts := strings.Split(newEntryId, "-")
	existingParts := strings.Split(existingEntryId, "-")

	if len(newParts) != 2 || len(existingParts) != 2 {
		return false, errors.New("invalid entry ID format")
	}

	newTimestamp, err := strconv.Atoi(newParts[0])
	if err != nil {
		return false, errors.New("invalid timestamp in entry ID")
	}

	newSequence, err := strconv.Atoi(newParts[1])
	if err != nil {
		return false, errors.New("invalid sequence number in entry ID")
	}

	existingTimestamp, err := strconv.Atoi(existingParts[0])
	if err != nil {
		return false, errors.New("invalid timestamp in entry ID")
	}

	existingSequence, err := strconv.Atoi(existingParts[1])
	if err != nil {
		return false, errors.New("invalid sequence number in entry ID")
	}

	if newTimestamp < existingTimestamp || (newTimestamp == existingTimestamp && newSequence <= existingSequence) {
		return false, nil
	}

	return true, nil
}

func (s *StreamStorage) XReadStreamsBlock(streamKeyName string, entryIDName string, timeout int, streamTopicName ...string) (map[string][]StreamEntry, error) {

	topicSubscribed := parserModel.XREAD_TOPIC + "_" + streamKeyName
	subChan := events.GetPubSub().Subscribe(topicSubscribed)
	var entries map[string][]StreamEntry = make(map[string][]StreamEntry)

	defer func() {
		events.GetPubSub().Unsubscribe(topicSubscribed, subChan)
		if r := recover(); r != nil {
			log.LogError(errors.New("panic occurred"))
		}
		if len(streamTopicName) > 0 {
			events.GetPubSub().Publish(events.Event{
				Topic: streamTopicName[0],
				Data:  "STOP",
			})
		}
	}()

	// Max Timeout value is 2^31-1 milliseconds
	if timeout < 0 {
		return nil, errors.New("timeout value should be greater than 0")
	}

	if timeout > 2147483647 {
		return nil, errors.New("timeout value should be less than 2147483647")
	}

	// Wait for new entries or timeout
	timeoutTx := time.After(time.Duration(timeout) * time.Millisecond)

	for {
		select {
		case <-timeoutTx:
			return entries, nil // Return collected entries on timeout

		case event := <-subChan:
			log.LogInfo(fmt.Sprintf("Received event: %v", event))
			// Process the received event
			parts := strings.Split(event.Topic, "_")
			if len(parts) < 3 {
				log.LogError(errors.New("invalid topic format"))
				continue // Invalid topic format, skip processing
			}
			streamKey := parts[2]
			newStreamEntry, ok := event.Data.(StreamEntry)
			if !ok {
				log.LogError(errors.New("invalid event data format"))
				continue // Invalid event data format, skip processing
			}

			var isInRange bool
			var err error

			if entryIDName == parserModel.XREAD_COMMAND_DOLLAR {
				isInRange = true
			} else {
				// Check if received entry is greater than the entryIDName
				isInRange, err = isEntryInRange(newStreamEntry.ID, entryIDName)
				if err != nil {
					log.LogError(err)
					continue
				}
			}

			if isInRange {
				entries[streamKey] = append(entries[streamKey], newStreamEntry)
				if len(streamTopicName) > 0 {
					// Publish the received entry to the streamTopicName
					log.LogInfo(fmt.Sprintf("Publishing event to topic: %v", streamTopicName[0]))
					eventData := events.Event{
						Topic: streamTopicName[0],
						Data:  newStreamEntry,
					}
					events.GetPubSub().Publish(eventData)
				}
			}
		}
	}
}
