package geecache

import (
	"fmt"
	"log"
	"sync"

	pb "github.com/yoeelingBin/bincache/geecache/geecachepb"
	"github.com/yoeelingBin/bincache/geecache/singleflight"
)

// Getter is the interface to load data of the key
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc is the function to implement the Getter interface
type GetterFunc func(key string) ([]byte, error)

// Get is the function to call the GetterFunc
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// Group is the cache namespace and associated data
type Group struct {
	name      string              // name of the cache
	getter    Getter              // callback to get data
	mainCache cache               // main cache
	peers     PeerPicker          // peer picker
	loader    *singleflight.Group // avoid cache breakdown
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// GetGroup is used to get the group by the name(readonly)
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get is used to get the value of the key
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}

	return g.load(key)
}

// load the value of the key
func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err := g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// getLocally get the value of the key locally
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key) // call the callback to get the data
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// populateCache is used to add the key and value to the cache
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// getFromPeer gets the value of the key from the peer
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: []byte(res.Value)}, nil
}
