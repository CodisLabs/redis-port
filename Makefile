.DEFAULT_GOAL := build-all

export GO15VENDOREXPERIMENT=0

.PHONY: godep

GODEP :=

ifndef GODEP
GODEP := $(shell \
	if which godep 2>&1 >/dev/null; then \
		echo "godep"; \
	else \
		if [ ! -x "${GOPATH}/bin/godep" ]; then \
			go get -u github.com/tools/godep; \
		fi; \
		echo "${GOPATH}/bin/godep"; \
	fi;)
endif

build-all: redis-port

godep:
	@mkdir -p bin
	@GOPATH=`${GODEP} path` ${GODEP} restore -v 2>&1 | while IFS= read -r line; do echo "  >>>> $${line}"; done
	@echo

redis-port: godep
	${GODEP} go build -i -o bin/redis-port ./cmd

clean:
	@rm -rf bin

distclean: clean
	@rm -rf Godeps/_workspace/pkg

gotest: godep
	${GODEP} go test ./pkg/...
