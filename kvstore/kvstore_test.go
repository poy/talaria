package kvstore_test

import (
	"fmt"

	"github.com/apoydence/talaria/kvstore"
	"github.com/hashicorp/consul/api"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Kvstore", func() {
	var (
		clientAddr    string
		kv            *kvstore.KVStore
		key           string
		keyWithPrefix string
	)

	BeforeEach(func() {
		clientAddr = "some-addr"
		kv = kvstore.New(clientAddr)
		key = "some-key"
		keyWithPrefix = fmt.Sprintf("%s-%s", kvstore.Prefix, key)
	})

	AfterEach(func() {
		_, err := consulClient.KV().DeleteTree(kvstore.Prefix, nil)
		Expect(err).ToNot(HaveOccurred())

		sessions, _, err := consulClient.Session().List(nil)
		for _, session := range sessions {
			_, err = consulClient.Session().Destroy(session.ID, nil)
			Expect(err).ToNot(HaveOccurred())
		}
	})

	Context("Announcements", func() {
		It("Invokes callback when announcement is made", func() {
			results := make(chan string, 10)

			kv.ListenForAnnouncements(func(value string) {
				results <- value
			})

			kv.Announce("some-name-1")
			kv.Announce("some-name-1")
			kv.Announce("some-name-2")
			Eventually(results).Should(Receive(Equal("some-name-1")))
			Eventually(results).Should(Receive(Equal("some-name-1")))
			Eventually(results).Should(Receive(Equal("some-name-2")))
		})

		It("Invokes callback when announcement is already made", func() {
			results := make(chan string, 10)

			kv.Announce("some-name-1")

			kv.ListenForAnnouncements(func(value string) {
				results <- value
			})
			Eventually(results).Should(Receive(Equal("some-name-1")))
		})
	})

	Context("Acquire", func() {
		It("Saves a KV with a prefix", func(done Done) {
			defer close(done)
			acquired := kv.Acquire(key)

			pair, _, err := consulClient.KV().Get(keyWithPrefix, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(pair).ToNot(BeNil())
			Expect(pair.Key).To(Equal(keyWithPrefix))
			Expect(pair.Value).To(Equal([]byte(clientAddr)))
			Expect(pair.Session).ToNot(BeEmpty())
			Expect(acquired).To(BeTrue())
		})

		It("Does not overwrite a key if there is already a value", func(done Done) {
			defer close(done)
			session, _, err := consulClient.Session().CreateNoChecks(nil, nil)
			Expect(err).ToNot(HaveOccurred())

			pair := &api.KVPair{
				Key:     keyWithPrefix,
				Session: session,
				Value:   []byte("127.0.0.2"),
			}

			_, _, err = consulClient.KV().Acquire(pair, nil)
			Expect(err).ToNot(HaveOccurred())

			acquired := kv.Acquire(key)

			pair, _, err = consulClient.KV().Get(keyWithPrefix, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(pair).ToNot(BeNil())
			Expect(pair.Key).To(Equal(keyWithPrefix))
			Expect(pair.Value).To(Equal([]byte("127.0.0.2")))
			Expect(acquired).To(BeFalse())
		})

		It("Registers a session with a healthcheck", func() {
			sessions, _, err := consulClient.Session().List(nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(sessions).To(HaveLen(1))
			Expect(sessions[0].Checks).To(HaveLen(1))
		})

	})

	Context("Get Leader", func() {
		Context("FetchLeader", func() {
			It("returns false for a leaderless key", func() {
				_, ok := kv.FetchLeader(key)
				Expect(ok).To(BeFalse())
			})

			It("returns true and the leader if there is a leader", func(done Done) {
				defer close(done)
				expectedLeader := "127.0.0.2"
				session, _, err := consulClient.Session().CreateNoChecks(nil, nil)
				Expect(err).ToNot(HaveOccurred())

				pair := &api.KVPair{
					Key:     keyWithPrefix,
					Session: session,
					Value:   []byte(expectedLeader),
				}

				_, _, err = consulClient.KV().Acquire(pair, nil)
				Expect(err).ToNot(HaveOccurred())

				results := make(chan string, 1)
				go func() {
					for {
						leader, ok := kv.FetchLeader(key)
						if !ok {
							continue
						}
						results <- leader
						return
					}
				}()

				Eventually(results).Should(Receive(Equal(expectedLeader)))
			})
		})

		Context("ListenForLeader", func() {
			It("Invokes callback when a leader is elected", func(done Done) {
				defer close(done)
				nameCh := make(chan string, 100)
				uriCh := make(chan string, 100)
				kv.ListenForLeader(key, func(name, uri string) {
					nameCh <- name
					uriCh <- uri
				})

				expectedLeader := "127.0.0.2"
				session, _, err := consulClient.Session().CreateNoChecks(nil, nil)
				Expect(err).ToNot(HaveOccurred())

				pair := &api.KVPair{
					Key:     keyWithPrefix,
					Session: session,
					Value:   []byte(expectedLeader),
				}

				_, _, err = consulClient.KV().Acquire(pair, nil)
				Expect(err).ToNot(HaveOccurred())

				Eventually(nameCh).Should(Receive(Equal(key)))
				Eventually(uriCh).Should(Receive(Equal(expectedLeader)))
			}, 5)

			It("Invokes callback if a leader is already elected", func(done Done) {
				defer close(done)
				expectedLeader := "127.0.0.2"
				session, _, err := consulClient.Session().CreateNoChecks(nil, nil)
				Expect(err).ToNot(HaveOccurred())

				pair := &api.KVPair{
					Key:     keyWithPrefix,
					Session: session,
					Value:   []byte(expectedLeader),
				}

				_, _, err = consulClient.KV().Acquire(pair, nil)
				Expect(err).ToNot(HaveOccurred())

				nameCh := make(chan string, 100)
				uriCh := make(chan string, 100)
				kv.ListenForLeader(key, func(name, uri string) {
					nameCh <- name
					uriCh <- uri
				})

				Eventually(nameCh).Should(Receive(Equal(key)))
				Eventually(uriCh).Should(Receive(Equal(expectedLeader)))
			}, 5)
		})
	})

})
