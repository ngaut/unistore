GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO        := go
GOBUILD   := $(GO) build $(BUILD_FLAG)

default: node

node: tidb-parser
	$(GOBUILD) -ldflags "-X main.gitHash=`git rev-parse HEAD`" -o node/node node/main.go

tidb-parser: vendor/github.com/pingcap/tidb/parser/parser.go

vendor/github.com/pingcap/tidb/parser/parser.go:
	$(MAKE) -C vendor/github.com/pingcap/tidb parser
