package stanlee_test

import (
	"errors"
	"testing"

	"github.com/ariefdarmawan/byter"
	"github.com/kanoteknologi/stanlee"
	"github.com/smartystreets/goconvey/convey"
)

var (
	host = "nats://localhost:4222"
)

func TestNats(t *testing.T) {
	var (
		e          error
		sub1, sub2 stanlee.Sub
		pub        stanlee.Pub
	)
	convey.Convey("connect to host as subs", t, func() {
		bt := byter.NewByter("")
		sub1, e = stanlee.NewNats("s1", host, "test", "secret", bt)
		convey.So(e, convey.ShouldBeNil)
		defer sub1.Close()

		sub2, e = stanlee.NewNats("s2", host, "test", "secret", bt)
		convey.So(e, convey.ShouldBeNil)
		defer sub2.Close()

		fnHello := func(name string) string {
			return "Hello " + name
		}

		fnHelloWithError := func(name string) (string, error) {
			if name != "world" {
				return "", errors.New("name should be only world")
			}
			return "Hello " + name, nil
		}

		fnQue := func(workerName string) func(string) string {
			return func(name string) string {
				return "Hello " + name + "from " + workerName
			}
		}

		e1 := sub1.Subscribe("hello", false, fnHello)
		e2 := sub2.Subscribe("hello", false, fnHello)
		convey.So(e1, convey.ShouldBeNil)
		convey.So(e2, convey.ShouldBeNil)

		sub1.Subscribe("helloWithError", false, fnHelloWithError)

		sub1.Subscribe("helloQue", true, fnQue(sub1.ID()))
		sub2.Subscribe("helloQue", true, fnQue(sub2.ID()))

		convey.Convey("running a publisher", func() {
			pub, e = stanlee.NewNats("p1", host, "test", "secret", bt)
			convey.So(e, convey.ShouldBeNil)

			convey.Convey("normal publish", func() {
				res := ""
				e := pub.Publish("hello", "world", &res)
				convey.So(e, convey.ShouldBeNil)
				convey.So(res, convey.ShouldEqual, "Hello world")

				convey.Convey("publish with error", func() {
					res := ""
					e := pub.Publish("helloWithError", "arief", &res)
					convey.So(e, convey.ShouldNotBeNil)

					convey.Convey("publish que", func() {
						convey.Println("")
						for i := 0; i < 5; i++ {
							res := ""
							e := pub.Publish("helloQue", "world", &res)
							convey.So(e, convey.ShouldBeNil)
							convey.Println("result: ", res)
						}
					})
				})
			})
		})
	})
}
