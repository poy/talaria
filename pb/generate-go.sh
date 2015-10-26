#!/bin/bash

dir_resolve()
{
    cd "$1" 2>/dev/null || return $?  # cd to desired directory; if fail, quell any error $package but return exit status
    echo "`pwd -P`" # output full, link-resolved path
}

set -e

TARGET=`dirname $0`
TARGET=`dir_resolve $TARGET`
cd $TARGET

go get github.com/gogo/protobuf/{proto,protoc-gen-gogo,gogoproto}

for package in $(ls definitions); do
	rm -rf generate-go-tmp
	mkdir -p generate-go-tmp/$package
	mkdir $package

	for i in $(ls definitions/$package/*.proto); do
			cp go/go_preamble.proto generate-go-tmp/$package/`basename $i`
			cat $i >> generate-go-tmp/$package/`basename $i`
	done

	pushd generate-go-tmp/$package > /dev/null
	protoc --plugin=$(which protoc-gen-gogo) --gogo_out=$TARGET/$package --proto_path=$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf:. *.proto
	popd > /dev/null

	rm -r generate-go-tmp
done
