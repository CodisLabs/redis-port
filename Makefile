.DEFAULT_GOAL := build-all

build-all: redis-port

redis-port:
	go build -i -o bin/redis-port ./cmd

clean:
	@rm -rf bin

distclean: clean

gotest:
	go test ./pkg/...
