all: redis-port

godep-env:
	@command -v godep 2>&1 >/dev/null || go get -u github.com/tools/godep
	@GOPATH=`godep path` godep restore

redis-port: godep-env
	godep go build -i -o bin/redis-port ./cmd

clean:
	rm -rf bin

distclean: clean
	@rm -rf Godeps/_workspace

gotest:
	godep go test -cover -v ./...
