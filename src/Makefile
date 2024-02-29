ext = so

os = $(shell uname | awk '{print tolower($0)}')
machine = $(shell uname -m | awk '{print tolower($0)}')
ifneq ("", "${GOARCH}")
machine = ${GOARCH}
endif

fname = "${os}_${machine}_libvegas.so"

.PHONY: clean
clean: 
	rm -rf build

.PHONY: build
build: clean
	CGO_ENABLED=1 go build -buildmode=c-shared -o "build/${fname}" ./interop

.PHONY: link
link:
	ln -s "$(pwd)/build/build/${fname}" "../lang/python/src/${fname}"
