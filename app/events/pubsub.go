package events

// Event represents an event with a specific topic.
type Event struct {
	Topic string
	Data  interface{}
}

// PubSub represents a pub/sub system with support for topics.
type PubSub struct {
	subscribers map[string][]chan Event // Map of topics to subscriber channels
	newSub      chan subRequest         // Channel for new subscriber requests
	delSub      chan subRequest         // Channel for subscriber removal requests
	events      chan Event              // Channel for incoming events
}

// subRequest represents a subscription request.
type subRequest struct {
	topic string
	sub   chan Event
}

var pubSub *PubSub

func init() {
	pubSub = NewPubSub()
}

func GetPubSub() *PubSub {
	return pubSub
}

// NewPubSub creates a new instance of the pub/sub system.
func NewPubSub() *PubSub {
	ps := &PubSub{
		subscribers: make(map[string][]chan Event),
		newSub:      make(chan subRequest),
		delSub:      make(chan subRequest),
		events:      make(chan Event),
	}
	go ps.start()
	return ps
}

// start starts the pub/sub system's event processing loop.
func (ps *PubSub) start() {
	for {
		select {
		case req := <-ps.newSub:
			ps.subscribers[req.topic] = append(ps.subscribers[req.topic], req.sub)
		case req := <-ps.delSub:
			subscribers := ps.subscribers[req.topic]
			for i, sub := range subscribers {
				if sub == req.sub {
					// Remove the subscriber
					subscribers[i] = subscribers[len(subscribers)-1]
					subscribers = subscribers[:len(subscribers)-1]
					ps.subscribers[req.topic] = subscribers
					break
				}
			}
		case event := <-ps.events:
			topic := event.Topic
			sub, ok := ps.subscribers[topic]
			if !ok {
				continue
			}
			for _, subscriber := range sub {
				subscriber <- event
			}
		}
	}
}

// Subscribe subscribes to a specific topic to receive events.
func (ps *PubSub) Subscribe(topic string) chan Event {
	sub := make(chan Event)
	ps.newSub <- subRequest{topic: topic, sub: sub}
	return sub
}

// Unsubscribe unsubscribes from receiving events of a specific topic.
func (ps *PubSub) Unsubscribe(topic string, sub chan Event) {
	ps.delSub <- subRequest{topic: topic, sub: sub}
}

// Publish publishes an event with a specific topic.
func (ps *PubSub) Publish(event Event) {
	ps.events <- event // Send the event to the pub/sub system
}
