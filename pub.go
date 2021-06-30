package stanlee

type Pub interface {
	Publish(topic string, payload, replay interface{}) error
	Close()
}
