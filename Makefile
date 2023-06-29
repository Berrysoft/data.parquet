.PHONY: generate compile test

generate:
	clj -T:build generate

compile:
	clj -T:build compile

test: compile
	LD_LIBRARY_PATH=target/debug clj -X:test

.PHONY: compile-release test-release

compile-release:
	clj -T:build compile :release true

test-release: compile-release
	LD_LIBRARY_PATH=target/release clj -X:test

.PHONY: jar install
jar:
	clj -T:build jar

install:
	clj -T:build install

.SECONDARY:
