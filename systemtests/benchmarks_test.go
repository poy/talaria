package systemtests_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("Benchmarks", func() {

	Context("Single Broker", func() {

		var (
			session *gexec.Session
			client  *broker.Client
			URL     string
		)

		BeforeEach(func() {
			var err error
			tmpDir, err = ioutil.TempDir("/tmp", "systemtalaria")
			Expect(err).ToNot(HaveOccurred())
			URL, session = startTalaria(tmpDir)
			client = startClient(URL)
		})

		AfterEach(func() {
			session.Kill()
			session.Wait("10s", "100ms")

			Expect(os.RemoveAll(tmpDir)).To(Succeed())
			client.Close()
		})

		Measure("It should read and write to single file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				fileId, err := client.FetchFile("some-file")
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < 1000; i++ {
					_, err = client.WriteToFile(fileId, []byte{byte(i)})
					Expect(err).ToNot(HaveOccurred())
				}

				for i := 0; i < 1000; i++ {
					data, err := client.ReadFromFile(fileId)
					Expect(err).ToNot(HaveOccurred())
					Expect(data[0]).To(Equal(byte(i)))
				}
			})
			Expect(runtime.Seconds()).To(BeNumerically("<", 1))
		}, 1)

	})

	Context("Multiple Brokers", func() {

		var (
			tmpDirs  []string
			sessions []*gexec.Session
			client   *broker.Client
		)

		BeforeEach(func() {
			var URLs []string
			var err error
			for i := 0; i < 3; i++ {
				tmpDir, err = ioutil.TempDir("/tmp", "systemtalaria")
				Expect(err).ToNot(HaveOccurred())
				tmpDirs = append(tmpDirs, tmpDir)

				URL, session := startTalaria(tmpDir)
				URLs = append(URLs, URL)
				sessions = append(sessions, session)
			}
			client = startClient(URLs...)
		})

		AfterEach(func() {
			for _, session := range sessions {
				session.Kill()
				session.Wait("10s", "100ms")
			}

			for _, tmpDir := range tmpDirs {
				Expect(os.RemoveAll(tmpDir)).To(Succeed())
			}

			client.Close()
		})

		Measure("Writes and reads from multiple files 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				var wg sync.WaitGroup
				defer wg.Wait()

				runTest := func(name string) {
					fileId, err := client.FetchFile(name)
					Expect(err).ToNot(HaveOccurred())
					var wg1 sync.WaitGroup
					wg1.Add(1)

					go func() {
						defer wg1.Done()
						defer GinkgoRecover()
						for i := 0; i < 1000; i++ {
							_, err := client.WriteToFile(fileId, []byte{byte(i)})
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					go func() {
						defer GinkgoRecover()
						defer wg.Done()
						wg1.Wait()
						for i := 0; i < 1000; i++ {
							data, err := client.ReadFromFile(fileId)
							Expect(err).ToNot(HaveOccurred())
							Expect(data).To(HaveLen(1))
							Expect(data[0]).To(Equal(byte(i)))
						}
					}()
				}

				count := 5
				wg.Add(count)
				for i := 0; i < count; i++ {
					runTest(fmt.Sprintf("some-file-%d", i))
				}
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))
		}, 1)
	})
})
