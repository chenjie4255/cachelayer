package cachelayer

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
)

const (
	defaultGCPeriod = 60 * time.Second
)

type DebugLogger interface {
	Log(keyname string, hit bool, msg string)
}

type emptyLogger struct{}

func (l *emptyLogger) Log(keyname string, hit bool, msg string) {}

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
	cacheMap  *cache.Cache
	lifeTime  int64
	freshTime int64
	logger    DebugLogger
}

func New(freshTime, lifeTime time.Duration) *CacheLayer {
	layer := CacheLayer{}
	layer.cacheMap = cache.New(lifeTime, defaultGCPeriod)
	layer.freshTime = freshTime.Nanoseconds()
	layer.lifeTime = lifeTime.Nanoseconds()
	layer.logger = &emptyLogger{}

	return &layer
}

func decode(data []byte, output interface{}) {
	err := json.Unmarshal(data, output)
	if err != nil {
		panic(err)
	}
}

func (l *CacheLayer) SetDebugLogger(logger DebugLogger) {
	if logger != nil {
		l.logger = logger
	}
}

func (l *CacheLayer) Clear() {
	l.cacheMap = cache.New(time.Duration(l.lifeTime), defaultGCPeriod)
}

func (l *CacheLayer) ClearIfNilErr(err error) error {
	if err == nil {
		l.Clear()
	}
	return err
}

func encode(obj interface{}) []byte {
	data, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return data
}

func (l *CacheLayer) addCacheItem(key string, data interface{}) []byte {
	dataBytes := encode(data)
	storeItem := &cacheItem{}
	storeItem.data = dataBytes
	nowT := time.Now().UnixNano()
	storeItem.aliveAt = nowT + l.lifeTime
	storeItem.freshAt = nowT + l.freshTime

	l.cacheMap.Set(key, storeItem, defaultGCPeriod)
	return storeItem.data
}

func (l *CacheLayer) getCacheItem(key string) *cacheItem {
	val, found := l.cacheMap.Get(key)
	if !found {
		return nil
	}
	return val.(*cacheItem)
}

func (l *CacheLayer) update(key string, fetchFn FetchFunc) ([]byte, error) {
	// in alive period, just one update mission could be executed
	item := l.getCacheItem(key)
	if item != nil {
		if !atomic.CompareAndSwapInt32(&item.updating, 0, 1) {
			return nil, nil
		}
	}

	return l.fetch(key, fetchFn)
}

func (l *CacheLayer) fetch(key string, fetchFn FetchFunc) ([]byte, error) {
	fetchData, err := fetchFn()
	if err != nil {
		return nil, err
	}

	return l.addCacheItem(key, fetchData), nil
}

func (l *CacheLayer) Get(key string, output interface{}, fetchFn FetchFunc) error {
	item, found := l.cacheMap.Get(key)
	if found {
		cachedItem := item.(*cacheItem)
		if cachedItem.alive() {
			// in alive period
			decode(item.(*cacheItem).data, output)

			if !cachedItem.fresh() {
				l.logger.Log(key, true, "cache hit but need update.")
				go func() {
					l.update(key, fetchFn)
				}()
			} else {
				l.logger.Log(key, true, "cache hit.")
			}
			return nil
		}
	}

	l.logger.Log(key, false, "cache unhit or has expired.")

	data, err := l.fetch(key, fetchFn)
	if err != nil {
		return err
	}
	decode(data, output)
	return nil
}

func (l *CacheLayer) count() int {
	return l.cacheMap.ItemCount()
}
