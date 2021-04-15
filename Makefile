PROJECT=unistore
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

FAIL_ON_STDOUT      := awk '{ print } END { if (NR > 0) { exit 1 } }'

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -p 8
STATICCHECK         := GO111MODULE=on staticcheck

LDFLAGS             += -X "main.gitHash=`git rev-parse HEAD`" 
TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/ngaut/$(PROJECT)/||'
FILES               := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")

# Targets
.PHONY: build linux test go-build go-build-linux go-test check

default: build

test: go-test

go-test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	$(GOTEST) -race -cover $(PACKAGES)

go-build:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/unistore-server cmd/unistore-server/main.go

go-build-linux:
	GOOS=linux $(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/unistore-server-linux cmd/unistore-server/main.go

build: go-build

linux: go-build-linux

check: fmt errcheck unconvert lint tidy check-static vet staticcheck

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

errcheck:tools/bin/errcheck
	@echo "errcheck"
	@GO111MODULE=on tools/bin/errcheck -exclude ./tools/check/errcheck_excludes.txt -ignoretests -blank $(PACKAGES)

unconvert:tools/bin/unconvert
	@echo "unconvert check"
	@GO111MODULE=on tools/bin/unconvert ./...

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

check-static: tools/bin/golangci-lint
	@echo "golangci-lint"
	./tools/bin/golangci-lint run -v --disable-all --deadline=3m \
	  --enable=misspell \
	  --enable=ineffassign \
	  --enable=typecheck \
	  --enable=varcheck \
	  --enable=structcheck \
	  --enable=deadcode \
	  $$($(PACKAGE_DIRECTORIES))

vet:
	@echo "vet"
	$(GO) vet -all $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

staticcheck:
	@echo "staticcheck"
	$(GO) get honnef.co/go/tools/cmd/staticcheck
	$(STATICCHECK) ./...

tools/bin/errcheck: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/errcheck github.com/kisielk/errcheck

tools/bin/unconvert: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/unconvert github.com/mdempsky/unconvert

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ./tools/bin v1.29.0
