PROJECT=unistore
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -p 8

LDFLAGS             += -X "main.gitHash=`git rev-parse HEAD`" 
TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'

# Targets
.PHONY: build linux test go-build go-build-linux go-test

default: build

test: go-test

go-test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	$(GOTEST) -cover $(PACKAGES)

go-build:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/unistore-server cmd/unistore-server/main.go

go-build-linux:
	GOOS=linux $(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/unistore-server-linux cmd/unistore-server/main.go

build: go-build

linux: go-build-linux

compaction:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/compaction-server cmd/compaction-server/main.go
