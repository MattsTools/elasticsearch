.PHONY: build clean

build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/elasticsearch Elasticsearch/main.go

clean:
	rm -rf ./bin