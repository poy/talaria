package talaria_test

import (
	. "github.com/apoydence/talaria"
)

type mockFileGenerator func() WriteSeekCloser

func (m mockFileGenerator) CreateFile() WriteSeekCloser {
	return m()
}
