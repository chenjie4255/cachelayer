package cachelayer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBaseUsage(t *testing.T) {
	Convey("create database store", t, func() {
		store := make(map[string]string)
		store["ak"] = "av"
		store["bk"] = "bv"
		store["ck"] = "ck"

		key := "ak"
		fetchFn := func() (interface{}, error) {
			val, found := store[key]
			if !found {
				return nil, errors.New("key not found")
			}

			return val, nil
		}

		cache := New(10*time.Second, 15*time.Second)
		Convey("key 'ak' should can be retrieve", func() {
			check := ""
			err := cache.Get(key, &check, fetchFn)
			So(err, ShouldBeNil)
			So(check, ShouldEqual, "av")

			Convey("should hit cached when retrieve again", func() {
				check := ""
				err := cache.Get(key, &check, fetchFn)

				So(err, ShouldBeNil)
				So(check, ShouldEqual, "av")
			})
		})
	})
}

func TestConcurrency(t *testing.T) {
	Convey("create database store", t, func() {
		var fetchTimes int32
		fetchFn := func() (interface{}, error) {
			time.Sleep(1 * time.Millisecond) // need adding 1 millsecond delay since there is a slight concurrency problem(cause more than one times read) if reading op is too fast
			atomic.AddInt32(&fetchTimes, 1)
			return "ok", nil
		}

		cache := New(2*time.Second, 10*time.Second)

		Convey("retrieve for first time", func() {
			check := ""
			err := cache.Get("somekey", &check, fetchFn)

			So(err, ShouldBeNil)
			So(check, ShouldEqual, "ok")

			Convey("retrieve sencond times", func() {
				check := ""
				err := cache.Get("somekey", &check, fetchFn)

				So(err, ShouldBeNil)
				So(check, ShouldEqual, "ok")

				Convey("should call fetchFn just one times", func() {
					So(fetchTimes, ShouldEqual, 1)
				})

				Convey("wait until item become unfreshed", func() {
					time.Sleep(2 * time.Second)

					So(fetchTimes, ShouldEqual, 1)
					Convey("concurreny test", func(c C) {
						for i := 0; i < 5; i++ {
							go func() {
								check := ""
								err := cache.Get("somekey", &check, fetchFn)
								c.So(err, ShouldBeNil)
								c.So(check, ShouldEqual, "ok")
							}()
						}

						time.Sleep(1 * time.Second)
						So(fetchTimes, ShouldEqual, 2)
					})
				})
			})
		})
	})
}
