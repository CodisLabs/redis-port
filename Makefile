.DEFAULT_GOAL := build-all

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

jemalloc:
	@cd third_party/jemalloc && \
		./autogen.sh --with-jemalloc-prefix="je_" && make
