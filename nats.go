package stanlee

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/ariefdarmawan/byter"
	nats "github.com/nats-io/nats.go"
)

type natsHandler struct {
	nc *nats.Conn
	bt byter.Byter

	subs map[string]*nats.Subscription

	id, prefix, secret string
	defaultTimeout     time.Duration
}

func NewNats(id, host, prefix, secret string, bt byter.Byter) (*natsHandler, error) {
	if bt == nil {
		return nil, errors.New("byte enc/dec is not valid")
	}
	n := new(natsHandler)
	n.bt = bt
	nc, err := nats.Connect(host)
	if err != nil {
		return nil, errors.New("fail to connect to nats. " + err.Error())
	}
	n.nc = nc
	n.subs = make(map[string]*nats.Subscription)
	n.prefix = prefix
	n.secret = secret
	n.defaultTimeout = 3 * time.Second
	n.id = id
	return n, nil
}

func (n *natsHandler) ID() string {
	return n.id
}

func (n *natsHandler) Close() {
	for k, s := range n.subs {
		s.Unsubscribe()
		delete(n.subs, k)
	}
	n.nc.Close()
}

func (n *natsHandler) buildTopicName(topic string) string {
	if strings.HasPrefix(topic, "~") {
		topic = topic[1:]
	} else {
		if n.prefix != "" && !strings.HasPrefix(topic, n.prefix) {
			topic = path.Join(n.prefix, topic)
		}
	}
	if n.secret != "" {
		topic += "@" + n.secret
	}
	return topic
}

func (n *natsHandler) Publish(topic string, payload, reply interface{}) error {
	topic = n.buildTopicName(topic)
	bs, e := n.bt.Encode(payload)
	if e != nil {
		return errors.New("fail to encode payload. " + e.Error())
	}

	if reply == nil {
		if e = n.nc.Publish(topic, bs); e != nil {
			return errors.New("fail to send payload. " + e.Error())
		}
		return nil
	}

	var msg *nats.Msg
	timeout := n.defaultTimeout
	if msg, e = n.nc.Request(topic, bs, timeout); e != nil {
		return errors.New("fail to get reply. " + e.Error())
	}

	res := new(Respond)
	if e = n.bt.DecodeTo(msg.Data, res, nil); e != nil {
		return errors.New("fail to decode reply. " + e.Error())
	}

	if res.Error != "" {
		return errors.New("publish error. " + res.Error)
	}

	if bs, e := n.bt.Encode(res.Data); e != nil {
		return e
	} else {
		if e := n.bt.DecodeTo(bs, reply, nil); e != nil {
			return e
		}
	}

	return nil
}

func (n *natsHandler) Subscribe(topic string, exclusive bool, fn interface{}) error {
	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		return fmt.Errorf("fn should be a function")
	}

	topic = n.buildTopicName(topic)

	var (
		sc *nats.Subscription
		e  error
	)

	if !exclusive {
		sc, e = n.nc.Subscribe(topic, n.buildSubscribeFn(vfn))
	} else {
		sc, e = n.nc.QueueSubscribe(topic, n.secret, n.buildSubscribeFn(vfn))
	}

	if e != nil {
		return errors.New("fail to subscribe. %s" + e.Error())
	}

	if sub, ok := n.subs[topic]; ok {
		sub.Unsubscribe()
	}
	n.subs[topic] = sc

	return nil
}

func (n *natsHandler) Unsubcribe(topic string) {
	topic = n.buildTopicName(topic)
	if s, ok := n.subs[topic]; ok {
		s.Unsubscribe()
		delete(n.subs, topic)
	}
}

func (n *natsHandler) buildSubscribeFn(vfn reflect.Value) func(*nats.Msg) {
	var tparm reflect.Type

	hasInput := false
	tfn := vfn.Type()
	parmIsPtr := false

	if tfn.NumIn() > 0 {
		tparm = tfn.In(0)
		if tparm.String()[0] == '*' {
			parmIsPtr = true
			tparm = tparm.Elem()
		}
		hasInput = true
	}

	return func(msg *nats.Msg) {
		var parmPtr interface{}
		if hasInput {
			parmPtr = reflect.New(tparm).Interface()
			e := n.bt.DecodeTo(msg.Data, parmPtr, nil)
			if e != nil {
				return
			}
		}

		var (
			vparm reflect.Value
			fnRes []reflect.Value
		)

		if hasInput {
			if parmIsPtr {
				vparm = reflect.ValueOf(parmPtr)
			} else {
				vparm = reflect.ValueOf(parmPtr).Elem()
			}
			fnRes = vfn.Call([]reflect.Value{vparm})
		} else {
			fnRes = vfn.Call([]reflect.Value{})
		}

		res := Respond{}

		if len(fnRes) > 0 {
			res.Data = fnRes[0].Interface()
		}

		if len(fnRes) > 1 {
			if e, ok := fnRes[1].Interface().(error); ok && e != nil {
				res.Error = e.Error()
			}
		}

		bs, e := n.bt.Encode(res)
		if e != nil {
			msg.Respond([]byte{})
		} else {
			msg.Respond(bs)
		}
	}
}
