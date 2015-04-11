package datapipeline_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestDatapipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Datapipeline Suite")
}
