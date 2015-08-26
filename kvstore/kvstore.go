package kvstore

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/apoydence/talaria/logging"
	"github.com/hashicorp/consul/api"
)

const (
	Prefix         = "Leader"
	AnnouncePrefix = "Announce"
	CheckName      = "TalariaNodeCheck"
)

type KVStore struct {
	log         logging.Logger
	client      *api.Client
	kv          *api.KV
	clientAddr  string
	sessionName string
}

func New(clientAddr string) *KVStore {
	log := logging.Log("KVStore")
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		log.Panic("Unable to create client", err)
	}

	return &KVStore{
		log:         log,
		client:      client,
		kv:          client.KV(),
		clientAddr:  clientAddr,
		sessionName: registerSession(client, log),
	}
}

func (k *KVStore) FetchLeader(key string) (string, bool) {
	pair, _, err := k.kv.Get(fmt.Sprintf("%s-%s", Prefix, key), nil)
	if err != nil {
		k.log.Panic("Error getting key", err)
	}

	if pair == nil {
		return "", false
	}

	return string(pair.Value), true
}

func (k *KVStore) Acquire(key string) bool {
	pair := &api.KVPair{
		Key:     fmt.Sprintf("%s-%s", Prefix, key),
		Value:   []byte(k.clientAddr),
		Session: k.sessionName,
	}

	ok, _, err := k.kv.Acquire(pair, nil)
	if err != nil {
		k.log.Panic("Error acquiring key", err)
	}

	return ok
}

func (k *KVStore) Announce(name string) {
	pair := &api.KVPair{
		Key:   fmt.Sprintf("%s-%d", AnnouncePrefix, rand.Int63()),
		Value: []byte(name),
	}

	_, err := k.kv.Put(pair, nil)
	if err != nil {
		k.log.Panic("Error putting key", err)
	}
}

func (k *KVStore) ListenForLeader(name string, callback func(name, uri string)) {
	go k.listenForLeader(name, callback)
}

func (k *KVStore) listenForLeader(name string, callback func(name, uri string)) {
	pairs, meta, err := k.kv.List(fmt.Sprintf("%s-%s", Prefix, name), nil)
	if err != nil {
		k.log.Panic("Unable to list keys", err)
	}

	for _, pair := range pairs {
		callback(stripLeaderPrefix(pair.Key), string(pair.Value))
	}

	options := api.QueryOptions{
		RequireConsistent: true,
	}
	for {
		options.WaitIndex = meta.LastIndex
		pairs, meta, err = k.kv.List(fmt.Sprintf("%s-%s", Prefix, name), &options)
		if err != nil {
			k.log.Error("Unable to get leader", err)
			return
		}
		for _, pair := range pairs {
			callback(stripLeaderPrefix(pair.Key), string(pair.Value))
		}
	}
}

func stripLeaderPrefix(key string) string {
	return key[len(Prefix)+1:]
}

func (k *KVStore) ListenForAnnouncements(callback func(name string)) {
	go k.listenForAnnouncements(callback)
}

func (k *KVStore) listenForAnnouncements(callback func(name string)) {
	pairs, meta, err := k.kv.List(AnnouncePrefix, nil)
	if err != nil {
		k.log.Panic("Unable to list keys", err)
	}

	for _, pair := range pairs {
		callback(string(pair.Value))
	}

	var options api.QueryOptions
	for {
		options.WaitIndex = meta.LastIndex
		pairs, meta, err = k.kv.List(AnnouncePrefix, &options)
		if err != nil {
			k.log.Error("Unable to list keys", err)
			return
		}

		for _, pair := range pairs {
			callback(string(pair.Value))
		}
	}
}

func registerSession(client *api.Client, log logging.Logger) string {
	go setupHealthCheckEndpoint(log)

	checkReg := &api.AgentCheckRegistration{
		Name: CheckName,
	}
	checkReg.AgentServiceCheck.HTTP = "http://localhost:9999"
	checkReg.AgentServiceCheck.Interval = "1s"

	err := client.Agent().CheckRegister(checkReg)
	if err != nil {
		log.Panic("Failed to register health check", err)
	}
	waitForHealthy(client.Health(), log)

	sessionEntry := &api.SessionEntry{
		Checks: []string{checkReg.Name},
	}

	session, _, err := client.Session().Create(sessionEntry, nil)
	if err != nil {
		log.Panic("Unable to create session", err)
	}

	return session
}

func waitForHealthy(health *api.Health, log logging.Logger) {
	for {
		checks, _, err := health.State("passing", nil)
		if err != nil {
			log.Panic("Unable to read health check status", err)
		}

		for _, check := range checks {
			if check.Name == CheckName {
				return
			}
		}

		time.Sleep(250 * time.Millisecond)
	}
}

var setupHealthCheckOnce sync.Once

func setupHealthCheckEndpoint(log logging.Logger) {
	setupHealthCheckOnce.Do(func() {
		http.HandleFunc("/", serviceHealthCheck)
		err := http.ListenAndServe(":9999", nil)
		if err != nil {
			log.Panic("Unable to start health check listener", err)
		}
	})
}

func serviceHealthCheck(http.ResponseWriter, *http.Request) {
}
