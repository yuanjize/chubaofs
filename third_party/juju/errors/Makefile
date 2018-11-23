default: check

check:
	go test && go test -compiler gccgo

docs:
	godoc2md juju/errors > README.md
	sed -i 's|\[godoc-link-here\]|[![GoDoc](https://godoc.org/juju/errors?status.svg)](https://godoc.org/juju/errors)|' README.md


.PHONY: default check docs
