.PHONY: test race vet build demo bench check

test:
	go test ./... -count=1 -timeout 600s

race:
	go test -race -count=1 ./... -timeout 600s

vet:
	go vet ./...

build:
	go build -o bin/ ./cmd/...

demo:
	go run ./cmd/lab demo

bench:
	go run ./cmd/lab bench

check:
	go run ./cmd/lab check
