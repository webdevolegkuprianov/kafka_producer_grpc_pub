.PHONY: build
build:
	go build -v ./cmd/producer

.DEFAULT_GOAL := build