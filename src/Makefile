os = $(shell uname | tr [:upper:] [:lower:])
machine = ${GOARCH}
ifeq ("", "${machine}")
machine = $(shell uname -m | tr [:upper:] [:lower:])
endif

fname = "${os}_${machine}_libvegas.so"

.PHONY: clean
clean: 
	rm -rf build

.PHONY: build
build:
	CGO_ENABLED=1 go build -buildmode=c-shared -o "build/${fname}" ./interop

.PHONY: link
link:
	ln -s "$(pwd)/build/build/${fname}" "../lang/python/src/${fname}"
