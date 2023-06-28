.PHONY: generate compile test

generate:
	clj -T:build generate

compile:
	clj -T:build compile-debug

test: compile
	LD_LIBRARY_PATH=target/debug clj -X:test

.PHONY: compile-release test-release

compile-release:
	clj -T:build compile-release

test-release: compile-release
	LD_LIBRARY_PATH=target/release clj -X:test

.PHONY: jar
jar:
	clj -T:build jar

.SECONDARY:
