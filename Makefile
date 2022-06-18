.PHONY: lint format deps dev_deps test clean work

lint:
	staticcheck ./...
	go vet ./...
	golangci-lint run --go=1.18
	yarn prettier --check .

format:
	gofmt -s -w .
	yarn prettier --write .

deps:
	go mod download

dev_deps:
	go install honnef.co/go/tools/cmd/staticcheck@2022.1
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	yarn

test:
	go test -count=1 -cover -parallel $$(nproc) -race ./...

clean:
	rm -rf node_modules

work:
	echo "go 1.18\n\nuse (\n\t.\n\t../Common\n)" > go.work
	go mod tidy
