SHELL := /bin/bash
COMPOSE ?= docker compose

.PHONY: help up demo test test-unit test-smoke down status benchmark freshness \
        demo-kill-tm demo-spike demo-savepoint-upgrade demo-reprocess

help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-26s\033[0m %s\n", $$1, $$2}'

up: ## Build images, start 3-broker KRaft Kafka 4.4, Flink 2.2, ClickHouse 26.8, MinIO, Karapace, Grafana, and submit streaming jobs
	$(COMPOSE) up -d --build --wait kafka-1 kafka-2 kafka-3 schema-registry minio clickhouse flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 grafana
	$(COMPOSE) up --build flink-job-submitter
	$(COMPOSE) --profile tools build tools
	@echo ""
	@echo "Platform ready:"
	@echo "  Grafana Dashboard : http://localhost:$${GRAFANA_PORT:-3000}/d/clickstream-realtime-v1"
	@echo "  Flink Web UI      : http://localhost:$${FLINK_UI_PORT:-8081}"
	@echo "  ClickHouse HTTP   : http://localhost:$${CLICKHOUSE_HTTP_PORT:-8123}"
	@echo "  Schema Registry   : http://localhost:$${SCHEMA_REGISTRY_PORT:-8085}"
	@echo "  MinIO S3 Console  : http://localhost:$${MINIO_CONSOLE_PORT:-9001}"

demo: ## Generate realistic e-commerce sessions, flash-sale catalogue updates, duplicates & late events, then verify freshness and batch recount
	$(COMPOSE) run --rm tools python -m generator.clickstream_gen.run_demo \
		--duration-sec $${DEMO_DURATION_SEC:-75} \
		--events-per-sec $${DEMO_EPS:-250} \
		--duplicate-rate 0.03 \
		--late-rate 0.02 \
		--spike-multiplier 4.0

test-unit: ## Run Flink JUnit 5 MiniCluster SQL/DataStream tests and Python generator/recount unit tests (no Docker stack required)
	mvn -B -f flink/pom.xml test
	pytest -q generator/tests

test-smoke: ## Verify running cluster health, streaming vs batch recount exact match, and end-to-end freshness SLA
	$(COMPOSE) run --rm tools python scripts/verify_recount.py --wait-for-windows --strict
	$(COMPOSE) run --rm tools python scripts/measure_freshness.py --probe-count 150

test: test-unit ## Run full unit test suite; if the Docker stack is running, also execute live streaming-vs-batch recount verification
	@if $(COMPOSE) ps --status running clickhouse 2>/dev/null | grep -q clickhouse; then \
		echo "Running live streaming-vs-batch correctness verification against active stack..."; \
		$(COMPOSE) run --rm tools python scripts/verify_recount.py --strict; \
	else \
		echo "Docker stack not currently running; unit & MiniCluster tests completed."; \
	fi

benchmark: ## Benchmark Kafka 4.4 producer compression (none, lz4, zstd) x linger.ms x batch.size with live broker measurements
	$(COMPOSE) run --rm tools python scripts/benchmark_producer.py

freshness: ## Measure end-to-end p50/p95/p99 freshness (event creation -> ClickHouse -> Grafana HTTP query) and read_committed isolation
	$(COMPOSE) run --rm tools python scripts/measure_freshness.py --probe-count 300 --test-aborted-txn

status: ## Report Kafka consumer lag, Flink operator backpressure, and ClickHouse serving counts
	$(COMPOSE) run --rm tools python scripts/ops/monitor_lag_and_backpressure.py

demo-kill-tm: ## Failure Demo 1: Kill a Flink TaskManager under load, verify S3/RocksDB checkpoint restore and 0-diff batch recount
	bash scripts/demos/demo_1_kill_taskmanager.sh

demo-spike: ## Failure Demo 2: Push a 10x traffic spike, observe Flink backpressure + consumer lag buildup and drain recovery
	bash scripts/demos/demo_2_traffic_spike_10x.sh

demo-savepoint-upgrade: ## Failure Demo 3: Trigger S3 savepoint and upgrade stateful query v1 -> v2 without losing cumulative user state
	bash scripts/demos/demo_3_savepoint_query_upgrade.sh

demo-reprocess: ## Failure Demo 4: Truncate serving tables and reprocess Kafka from earliest offset to rebuild every metric identically
	bash scripts/demos/demo_4_reprocess_earliest.sh

down: ## Stop all containers and remove volumes
	$(COMPOSE) down -v --remove-orphans
