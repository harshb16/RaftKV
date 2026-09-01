export GOCACHE := /tmp/raftkv-go-cache
export GOPATH := /tmp/raftkv-go
.PHONY: build test race vet fmt check
build:
	mkdir -p bin
	go build -o bin/raftkv ./cmd/raftkv
test:
	go test ./...
race:
	go test -race ./...
vet:
	go vet ./...
fmt:
	gofmt -w cmd internal tests
check: test race vet
