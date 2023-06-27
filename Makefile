.PHONY: generate compile test

generate:
	clj -T:build generate

compile:
	clj -T:build jar

test: compile
	LD_LIBRARY_PATH=target/debug clj -X:test

.SECONDARY:
