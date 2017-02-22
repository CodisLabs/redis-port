.DEFAULT_GOAL := build-all

export GO15VENDOREXPERIMENT=1

build-all: redis-port

build-deps:
	@mkdir -p bin && bash version

redis-port: build-deps
	go build -i -o bin/redis-port ./cmd

clean:
	@rm -rf bin

distclean: clean

gotest:
	go test ./pkg/...
