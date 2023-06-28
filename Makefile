.PHONY: generate compile test

generate:
	clj -T:build generate

compile:
	clj -T:build jar-debug

test: compile
	LD_LIBRARY_PATH=target/debug clj -X:test

.PHONY: compile-release test-release

compile-release:
	clj -T:build jar-release

test-release: compile-release
	LD_LIBRARY_PATH=target/release clj -X:test

.SECONDARY:
