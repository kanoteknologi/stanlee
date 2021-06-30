package stanlee

type Sub interface {
	ID() string
	Subscribe(topic string, exclusive bool, fn interface{}) error
	Unsubcribe(topic string)
	Close()
}
