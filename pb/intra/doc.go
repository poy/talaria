package intra

//go:generate bash -c "protoc *.proto -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf --go_out=plugins=grpc:."
