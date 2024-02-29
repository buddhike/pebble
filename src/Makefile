fname = libvegas.so

.PHONY: clean
clean: 
	rm -rf build

.PHONY: build
build: clean
	go build -buildmode=c-shared -o "build/${fname}" ./interop

.PHONY: link
link:
	ln -s "$(pwd)/build/build/${fname}" "../lang/python/src/${fname}"
