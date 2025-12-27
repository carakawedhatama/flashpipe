.PHONY: all build run test clean docker-up docker-down loadtest prometheus help

BINARY_NAME=flashpipe
MAIN_PATH=./cmd/ingest-api
CONSUMER_PATH=./cmd/consumer
LOADTEST_PATH=./scripts/loadtest.go

all: build

build:
	@echo "🔨 Building FlashPipe..."
	go build -o bin/$(BINARY_NAME) $(MAIN_PATH)
	go build -o bin/consumer $(CONSUMER_PATH)
	@echo "✅ Build complete"

run:
	@echo "🚀 Starting FlashPipe..."
	go run $(MAIN_PATH)

run-consumer:
	@echo "🚀 Starting Consumer..."
	go run $(CONSUMER_PATH)

test:
	@echo "🧪 Running tests..."
	go test ./... -v -cover

test-short:
	go test ./... -short

test-coverage:
	@echo "📊 Generating coverage report..."
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "✅ Coverage report: coverage.html"

docker-up:
	@echo "🐳 Starting Docker services..."
	docker-compose up -d redpanda redis
	@echo "⏳ Waiting for services to be ready..."
	sleep 5
	@echo "✅ Services ready"

docker-down:
	@echo "🛑 Stopping Docker services..."
	docker-compose down

docker-all:
	@echo "🐳 Starting all services including FlashPipe..."
	docker-compose up -d

prometheus:
	@echo "📈 Starting Prometheus..."
	docker run -d --name prometheus \
		-p 9090:9090 \
		-v $(PWD)/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml \
		--network host \
		prom/prometheus:latest
	@echo "✅ Prometheus running at http://localhost:9090"

prometheus-stop:
	docker stop prometheus && docker rm prometheus

grafana:
	@echo "📊 Starting Grafana..."
	docker run -d --name grafana \
		-p 3000:3000 \
		-e GF_SECURITY_ADMIN_PASSWORD=admin \
		--network host \
		grafana/grafana:latest
	@echo "✅ Grafana running at http://localhost:3000 (admin/admin)"

grafana-stop:
	docker stop grafana && docker rm grafana

loadtest:
	@echo "🔥 Running load test (60s, 100 concurrent)..."
	go run $(LOADTEST_PATH) -duration=60s -concurrency=100 -mode=mixed

loadtest-light:
	@echo "🔥 Running light load test (30s, 10 concurrent)..."
	go run $(LOADTEST_PATH) -duration=30s -concurrency=10 -mode=mixed

loadtest-heavy:
	@echo "🔥 Running heavy load test (120s, 500 concurrent)..."
	go run $(LOADTEST_PATH) -duration=120s -concurrency=500 -batch-size=500 -mode=batch

loadtest-single:
	@echo "🔥 Running single event load test..."
	go run $(LOADTEST_PATH) -duration=60s -concurrency=50 -mode=single

loadtest-batch:
	@echo "🔥 Running batch event load test..."
	go run $(LOADTEST_PATH) -duration=60s -concurrency=50 -batch-size=100 -mode=batch

dev: docker-up run

lint:
	@echo "🔍 Running linter..."
	golangci-lint run ./...

fmt:
	@echo "✨ Formatting code..."
	go fmt ./...

tidy:
	@echo "📦 Tidying dependencies..."
	go mod tidy

clean:
	@echo "🧹 Cleaning..."
	rm -rf bin/ coverage.out coverage.html
	go clean

help:
	@echo "FlashPipe - High-Throughput Event Ingestion System"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build          Build the binaries"
	@echo "  run            Run the API server"
	@echo "  run-consumer   Run the event consumer"
	@echo "  test           Run all tests with coverage"
	@echo "  test-coverage  Generate HTML coverage report"
	@echo ""
	@echo "  docker-up      Start Redis and Redpanda"
	@echo "  docker-down    Stop Docker services"
	@echo "  docker-all     Start all services including FlashPipe"
	@echo ""
	@echo "  prometheus     Start Prometheus monitoring"
	@echo "  grafana        Start Grafana dashboard"
	@echo ""
	@echo "  loadtest       Run standard load test (60s)"
	@echo "  loadtest-light Run light load test (30s)"
	@echo "  loadtest-heavy Run heavy load test (120s)"
	@echo ""
	@echo "  dev            Start dependencies and run API"
	@echo "  clean          Clean build artifacts"
	@echo "  help           Show this help"
