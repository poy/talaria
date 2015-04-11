package datapipeline_test

import (
	"github.com/apoydence/talaria/datapipeline"
)

type mockFileGenerator func() datapipeline.WriteSeekCloser

func (m mockFileGenerator) CreateFile() datapipeline.WriteSeekCloser {
	return m()
}
