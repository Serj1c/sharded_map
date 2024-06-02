package shardedmap

import (
	"fmt"
	"sync"
)

type ShardedMap struct {
	shards []*Shard
	Keys   int
}

type Shard struct {
	mu          sync.RWMutex
	InternalMap map[string]*interface{}
}

func NewShardedMap(shardAmount int) ShardedMap {
	if shardAmount < 1 {
		shardAmount = 1
	}

	sm := ShardedMap{
		shards: make([]*Shard, shardAmount),
		Keys:   0,
	}

	for i := 0; i < len(sm.shards); i++ {
		sm.shards[i] = &Shard{
			InternalMap: make(map[string]*interface{}),
		}
	}

	return sm
}

func (sm *ShardedMap) DjbHash(input string) uint32 {
	var hash uint32 = 5381 // majic number
	for _, ch := range input {
		hash = ((hash << 5) + hash) + uint32(ch)
	}
	return hash
}

func (sm *ShardedMap) Get(key string) *interface{} {
	shard := sm.DjbHash(key) & uint32(len(sm.shards)-1)
	if sm.shards[shard] == nil {
		panic("this should never happen!")
	}

	sm.shards[shard].mu.RLock()
	defer sm.shards[shard].mu.RUnlock()

	return sm.shards[shard].InternalMap[key]
}

func (sm *ShardedMap) Set(key string, data *interface{}) {
	shard := sm.DjbHash(key) & uint32(len(sm.shards)-1)
	if sm.shards[shard] == nil {
		panic("this should never happen!")
	}

	sm.shards[shard].mu.Lock()
	defer sm.shards[shard].mu.Unlock()

	sm.shards[shard].InternalMap[key] = data
	return
}

func (sm *ShardedMap) Delete(key string) {
	shard := sm.DjbHash(key) & uint32(len(sm.shards)-1)

	sm.shards[shard].mu.Lock()
	defer sm.shards[shard].mu.Unlock()

	delete(sm.shards[shard].InternalMap, key)
	return
}

func (sm *ShardedMap) DumpKeys() {
	fmt.Println("dump keys started")
	for shardId, shard := range sm.shards {
		shard.mu.Lock()
		for key := range shard.InternalMap {
			fmt.Println("shard key: ", key, "on shard: ", shardId)
		}
		shard.mu.Unlock()
	}
	fmt.Println("dump keys ended")
}
