package cachelayer

import (
	"errors"
	"math/rand"
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

			Convey("clear all cache", func() {
				cache.Clear()
			})

			Convey("clear if nil err", func() {
				So(cache.ClearIfNilErr(nil), ShouldBeNil)
				So(cache.ClearIfNilErr(errors.New("whatever")), ShouldNotBeNil)
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

		cache := New(2*time.Second, 5*time.Second)
		cache.SetStoreOriginData()

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

func BenchmarkReading(b *testing.B) {
	type StoreItem struct {
		Name string
		ID   string
	}
	cache := New(100*time.Second, 200*time.Second)
	fetchFn := func() (interface{}, error) {
		return StoreItem{"name", "id1"}, nil
	}
	for i := 0; i < b.N; i++ {
		val := StoreItem{}
		if rand.Intn(10) == 0 {
			cache.Get("nokey", &val, fetchFn)
		} else {
			cache.Get("key", &val, fetchFn)
		}
	}
}

func TestAssignValue(t *testing.T) {
	Convey("testing assign string ", t, func() {
		str := "ok"
		ret := "1"

		Convey("assign value", func() {
			assignValue(&ret, str)
			So(ret, ShouldEqual, "ok")
		})

		Convey("assign pointer", func() {
			assignValue(&ret, &str)
			So(ret, ShouldEqual, "ok")
		})
	})

	Convey("testing assign integer", t, func() {
		val := 1
		ret := 0

		Convey("assign value", func() {
			assignValue(&ret, val)
			So(ret, ShouldEqual, 1)
		})

		Convey("assign pointer", func() {
			assignValue(&ret, &val)
			So(ret, ShouldEqual, 1)
		})
	})

	Convey("testing assign slice", t, func() {
		val := make([]int, 3, 5)
		val[0] = 0
		val[1] = 1
		val[2] = 2
		ret := []int{}

		Convey("assign value", func() {
			assignValue(&ret, val)
			So(len(ret), ShouldEqual, 3)
			ret[0] = 100
			So(val[0], ShouldEqual, 100)
			val = append(val, 4)
			So(len(ret), ShouldEqual, 3)
			val[0] = 101
			So(ret[0], ShouldAlmostEqual, 101)
			// So(fmt.Sprintf("%p", &ret), ShouldEqual, fmt.Sprintf("%p", &val))
		})

		Convey("assign pointer", func() {
			assignValue(&ret, &val)
			So(len(ret), ShouldEqual, 3)

			ret[0] = 100
			So(val[0], ShouldEqual, 100)
		})
	})
}

func TestBuildKey(t *testing.T) {
	key1 := BuildKey("ZZZZ", 1, 2)
	key2 := BuildKey("ZZZZ", 1, 1)

	t.Logf("key1:%s\n", key1)
	t.Logf("key2:%s\n", key2)

	if key1 == key2 {
		t.Fatalf("key should not equal")
	}
}
