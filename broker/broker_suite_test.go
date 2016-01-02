package broker_test

import (
	"github.com/apoydence/talaria/logging"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestBroker(t *testing.T) {
	logging.SetLevel(logging.CRITICAL)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Broker Suite")
}

func convertToWs(URL string) string {
	return "ws" + URL[4:]
}
