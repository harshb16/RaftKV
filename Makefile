export GOCACHE := /tmp/raftkv-go-cache
export GOPATH := /tmp/raftkv-go
.PHONY: build test race vet fmt check
build:
	mkdir -p bin
	go build -o bin/raftkv ./cmd/raftkv
	go build -o bin/raftkv-chaos ./cmd/raftkv-chaos
	go build -o bin/raftkv-bench ./cmd/raftkv-bench
test:
	go test ./...
race:
	go test -race ./...
vet:
	go vet ./...
fmt:
	gofmt -w cmd internal tests
check: test race vet
.PHONY: cluster stop demo chaos
cluster: build
	python3 scripts/cluster.py start
stop:
	python3 scripts/cluster.py stop
demo: build
	python3 scripts/cluster.py demo
chaos: build
	./bin/raftkv-chaos -duration 10s -seed 42

.PHONY: bench
bench: build
	./bin/raftkv-bench -operations 300

.PHONY: cluster5 stop5 demo5
cluster5: build
	python3 scripts/cluster.py start --nodes 5
stop5:
	python3 scripts/cluster.py stop --nodes 5
demo5: build
	python3 scripts/cluster.py demo --nodes 5
