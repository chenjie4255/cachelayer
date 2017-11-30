package cachelayer

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"
)

type FetchFunc func() (interface{}, error)

type cacheItem struct {
	data     []byte
	freshAt  int64
	aliveAt  int64
	updating int32 // > 0 indicate this item is under updating
}

func (c cacheItem) fresh() bool {
	return time.Now().UnixNano() < c.freshAt
}

func (c cacheItem) alive() bool {
	return time.Now().UnixNano() < c.aliveAt
}

type CacheLayer struct {
	cacheMap  sync.Map
	lifeTime  int64
	freshTime int64
}

func New(freshTime, lifeTime time.Duration) *CacheLayer {
	layer := CacheLayer{}
	layer.freshTime = freshTime.Nanoseconds()
	layer.lifeTime = lifeTime.Nanoseconds()

	return &layer
}

func decode(data []byte, output interface{}) {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(output); err != nil {
		panic(err)
	}
}

func (l *CacheLayer) addCacheItem(key string, data interface{}) {
	dataBuf := new(bytes.Buffer)
	enc := gob.NewEncoder(dataBuf)
	if err := enc.Encode(data); err != nil {
		panic(err)
	}

	storeItem := &cacheItem{}
	storeItem.data = dataBuf.Bytes()
	nowT := time.Now().UnixNano()
	storeItem.aliveAt = nowT + l.lifeTime
	storeItem.freshAt = nowT + l.freshTime

	l.cacheMap.Store(key, storeItem)
}

func (l *CacheLayer) getCacheItem(key string) *cacheItem {
	val, found := l.cacheMap.Load(key)
	if !found {
		return nil
	}
	return val.(*cacheItem)
}

func (l *CacheLayer) update(key string, fetchFn FetchFunc, forced bool) error {
	if !forced {
		// in alive period, just one update mission could be executed
		item := l.getCacheItem(key)
		if item != nil {
			if !atomic.CompareAndSwapInt32(&item.updating, 0, 1) {
				return nil
			}
		}
	}

	fetchData, err := fetchFn()
	if err != nil {
		return err
	}

	l.addCacheItem(key, fetchData)
	return nil
}

func (l *CacheLayer) mustGetFromCached(key string, output interface{}) {
	item, found := l.cacheMap.Load(key)
	if found {
		cachedItem := item.(*cacheItem)
		if cachedItem.alive() {
			decode(item.(*cacheItem).data, output)
		} else {
			panic("cached key should be alive")
		}
	} else {
		panic("failed to get a must cached key")
	}
}

func (l *CacheLayer) Get(key string, output interface{}, fetchFn FetchFunc) error {
	item, found := l.cacheMap.Load(key)
	if found {
		cachedItem := item.(*cacheItem)
		if cachedItem.alive() {
			// in alive period
			decode(item.(*cacheItem).data, output)

			if !cachedItem.fresh() {
				go func() {
					l.update(key, fetchFn, false)
				}()
			}
			return nil
		}
	}

	if err := l.update(key, fetchFn, true); err != nil {
		return err
	}

	l.mustGetFromCached(key, output)
	return nil
}
