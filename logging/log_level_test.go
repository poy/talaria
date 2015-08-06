package logging_test

import (
	"github.com/apoydence/talaria/logging"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogLevel", func() {
	It("correctly marshals the LogLevel", func() {
		logLevel := logging.INFO
		json, err := logLevel.MarshalJSON()

		Expect(err).ToNot(HaveOccurred())
		Expect(json).To(Equal([]byte("INFO")))
	})

	It("correctly unmarshals the LogLevel", func() {
		var logLevel logging.LogLevel
		err := logLevel.UnmarshalJSON([]byte(`"DEBUG"`))

		Expect(err).ToNot(HaveOccurred())
		Expect(logLevel).To(Equal(logging.DEBUG))
	})
})
