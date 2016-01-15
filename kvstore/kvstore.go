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
	Prefix    = "Leader"
	CheckName = "TalariaNodeCheck"
)

type KVStore struct {
	log         logging.Logger
	client      *api.Client
	kv          *api.KV
	clientAddr  string
	sessionName string
}

func New(clientAddr string, healthPort int) *KVStore {
	log := logging.Log("KVStore")
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		log.Panic("Unable to create client", err)
	}

	healthCheckName := fmt.Sprintf("%s-%d", CheckName, rand.Int63())

	return &KVStore{
		log:         log,
		client:      client,
		kv:          client.KV(),
		clientAddr:  clientAddr,
		sessionName: registerSession(healthPort, healthCheckName, client, log),
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
	return k.tryAcquire(key)
}

func (k *KVStore) Release(name string) {
	k.log.Debug("(%s) Releasing %s", k.clientAddr, name)
	pair := &api.KVPair{
		Key:     fmt.Sprintf("%s-%s", Prefix, name),
		Session: k.sessionName,
	}

	ok, _, err := k.kv.Release(pair, nil)
	if !ok {
		k.log.Panicf("(%s) Failed to release key %s: expected return value 'true'", k.clientAddr, name)
	}

	if err != nil {
		k.log.Panicf("(%s) Failed to release key %s: %v", k.clientAddr, name, err.Error())
	}
}

func (k *KVStore) Announce(name string) {
	k.log.Debug("Announcing %s", name)
	pair := &api.KVPair{
		Key: fmt.Sprintf("%s-%s", Prefix, name),
	}

	_, err := k.kv.Put(pair, nil)
	if err != nil {
		k.log.Panicf("Error putting key %s: %v", name, err)
	}
}

func (k *KVStore) ListenForLeader(name string, callback func(name, uri string)) {
	go k.listenForLeader(name, callback)
}

func (k *KVStore) tryAcquire(key string) bool {
	pair := &api.KVPair{
		Key:     fmt.Sprintf("%s-%s", Prefix, key),
		Value:   []byte(k.clientAddr),
		Session: k.sessionName,
	}

	var ok bool
	var err error
	for i := 0; i < 5; i++ {
		ok, _, err = k.kv.Acquire(pair, nil)
		if err == nil {
			return ok
		}
		time.Sleep(time.Second)
	}

	k.log.Panicf("Error acquiring key %s: %v", key, err)
	return false
}

func (k *KVStore) listenForLeader(name string, callback func(name, uri string)) {
	pairs, meta, err := k.kv.List(fmt.Sprintf("%s-%s", Prefix, name), nil)
	if err != nil {
		k.log.Panic("Unable to list keys", err)
	}

	for _, pair := range pairs {
		if pair.Session != "" {
			callback(stripLeaderPrefix(pair.Key), string(pair.Value))
		}
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
			if pair.Session != "" {
				callback(stripLeaderPrefix(pair.Key), string(pair.Value))
			}
		}
	}
}

func (k *KVStore) ListenForAnnouncements(callback func(name string)) {
	go k.listenForAnnouncements(callback)
}

func stripLeaderPrefix(key string) string {
	return key[len(Prefix)+1:]
}

func findMatch(pairs api.KVPairs, name string) *api.KVPair {
	for _, pair := range pairs {
		if string(pair.Value) == name {
			return pair
		}
	}

	return nil
}

func (k *KVStore) listenForAnnouncements(callback func(name string)) {
	var options api.QueryOptions

	for {
		pairs, meta, err := k.kv.List(Prefix, &options)
		if err != nil {
			k.log.Error("Unable to list keys", err)
			return
		}
		options.WaitIndex = meta.LastIndex

		for _, pair := range pairs {
			if pair.Session == "" {
				callback(stripLeaderPrefix(pair.Key))
			}
		}
	}
}

func registerSession(healthPort int, healthCheckName string, client *api.Client, log logging.Logger) string {
	go setupHealthCheckEndpoint(healthPort, log)

	checkReg := &api.AgentCheckRegistration{
		Name: healthCheckName,
	}
	checkReg.AgentServiceCheck.HTTP = fmt.Sprintf("http://localhost:%d", healthPort)
	checkReg.AgentServiceCheck.Interval = "1s"

	err := client.Agent().CheckRegister(checkReg)
	if err != nil {
		log.Panic("Failed to register health check", err)
	}
	waitForHealthy(healthCheckName, client.Health(), log)

	sessionEntry := &api.SessionEntry{
		LockDelay: 1,
		Checks:    []string{checkReg.Name},
	}

	session, _, err := client.Session().Create(sessionEntry, nil)
	if err != nil {
		log.Panic("Unable to create session", err)
	}

	for {
		entry, _, err := client.Session().Info(session, nil)
		if err != nil {
			log.Panic("Unable to read session info", err)
		}
		if entry != nil {
			break
		}
	}

	return session
}

func waitForHealthy(healthCheckName string, health *api.Health, log logging.Logger) {
	for {
		checks, _, err := health.State("passing", nil)
		if err != nil {
			log.Panic("Unable to read health check status", err)
		}

		for _, check := range checks {
			if check.Name == healthCheckName {
				return
			}
		}

		time.Sleep(250 * time.Millisecond)
	}
}

var setupHealthCheckOnce sync.Once

func setupHealthCheckEndpoint(healthPort int, log logging.Logger) {
	setupHealthCheckOnce.Do(func() {
		uri := fmt.Sprintf(":%d", healthPort)
		log.Debug("Starting health check endpoint: %s", uri)
		err := http.ListenAndServe(uri, http.HandlerFunc(serviceHealthCheck))
		if err != nil {
			log.Panic("Unable to start health check listener", err)
		}
	})
}

func serviceHealthCheck(http.ResponseWriter, *http.Request) {
	// NOP
}
