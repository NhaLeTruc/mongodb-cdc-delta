# MongoDB CDC to Delta Lake Pipeline

Enterprise-grade Change Data Capture (CDC) pipeline that captures real-time changes from MongoDB and replicates them to Delta Lake tables on MinIO object storage.

## Features

- **Real-time CDC**: Capture MongoDB changes (insert/update/delete) with sub-minute latency
- **Schema Evolution**: Automatic schema adaptation without pipeline downtime
- **Reconciliation**: Scheduled and on-demand data integrity verification
- **High Throughput**: 10,000+ events/second per collection
- **Production Ready**: 99.9% availability, exactly-once delivery, comprehensive observability
- **Management API**: FastAPI-based REST API for pipeline configuration and monitoring
- **Analytics Ready**: Query Delta Lake tables using DuckDB

## Architecture

```
MongoDB → Debezium → Kafka → Delta Writer → MinIO (Delta Lake)
                                              ↓
                                          DuckDB Queries
                                              ↓
                                         FastAPI Management API
                                              ↓
                                    Prometheus + Grafana + Jaeger
```

## Quick Start

### Prerequisites

- Docker 24.0+ and Docker Compose 2.23+
- Python 3.11+
- 8GB+ RAM, 20GB+ disk space

### Local Development Setup

1. **Clone and setup**:
   ```bash
   git clone <repository-url>
   cd mongodb-cdc-delta
   ```

2. **Start all services**:
   ```bash
   make start
   ```
   Or directly with Docker Compose:
   ```bash
   docker compose up -d
   ```
   This starts: MongoDB, Kafka, Zookeeper, Kafka Connect, MinIO, PostgreSQL, Prometheus, Grafana, Jaeger, Vault

3. **Wait for services to be ready** (30-60 seconds):
   ```bash
   make health
   ```
   Or use the setup script:
   ```bash
   ./scripts/setup-local.sh
   ```

4. **Seed test data**:
   ```bash
   ./scripts/seed-mongodb.sh
   ```

5. **Create your first CDC pipeline**:
   ```bash
   ./scripts/create-pipeline.sh --collection users --database testdb
   ```

6. **Verify replication**:
   ```bash
   # Insert document in MongoDB
   docker exec mongodb mongosh testdb --eval 'db.users.insertOne({name: "Alice", email: "alice@example.com"})'

   # Query from Delta Lake via DuckDB (wait 10-30 seconds)
   ./scripts/query-deltalake.sh "SELECT * FROM users WHERE name = 'Alice'"
   ```

### Accessing UIs

- **Grafana**: http://localhost:3000 (admin/admin) - Monitoring dashboards
- **Jaeger**: http://localhost:16686 - Distributed tracing
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin) - Object storage
- **FastAPI Docs**: http://localhost:8000/docs - Management API

## Development

### Running Tests

```bash
# All tests
make test

# Unit tests only
make test-unit

# Integration tests
make test-integration

# E2E tests
make test-e2e

# Load tests
make test-load
```

### Code Quality

```bash
# Format code
make format

# Lint code
make lint

# Type checking
make typecheck

# Security scan
make security-scan

# All checks
make check
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
pre-commit install

# Run hooks manually
pre-commit run --all-files
```

## Project Structure

```
mongodb-cdc-delta/
├── api/                    # FastAPI management service
├── delta-writer/           # Kafka → Delta Lake writer
├── reconciliation/         # Reconciliation engine
├── shared/                 # Common utilities
├── tests/                  # E2E and cross-service tests
├── docker/                 # Docker Compose and configurations
│   ├── compose/            # docker-compose.yml files
│   ├── kafka-connect/      # Custom Kafka Connect image
│   └── monitoring/         # Prometheus, Grafana, Jaeger configs
├── config/                 # Configuration files
├── scripts/                # Utility scripts
└── docs/                   # Documentation
```

## Documentation

- [Architecture Overview](docs/architecture/overview.md)
- [API Documentation](docs/api/openapi.yaml)
- [Development Guide](docs/development/setup.md)
- [Testing Guide](docs/development/testing.md)
- [Runbooks](docs/runbooks/)
  - [Incident Response](docs/runbooks/incident-response.md)
  - [Reconciliation](docs/runbooks/reconciliation.md)
  - [Troubleshooting](docs/runbooks/troubleshooting.md)

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Throughput | ≥10,000 events/sec | Per collection |
| Latency (P95) | <60 seconds | End-to-end replication lag |
| Availability | 99.9% | ≤43 minutes downtime per month |
| Data Integrity | Zero data loss | Exactly-once semantics |
| Reconciliation | 1TB in <6 hours | With 8 workers |

## Monitoring & Observability

### Key Metrics

- **Replication Lag**: Time between MongoDB change and Delta Lake write
- **Throughput**: Events processed per second
- **Error Rate**: Failed writes per second
- **DLQ Size**: Dead letter queue depth
- **Reconciliation Status**: Discrepancies found, last run time

### Alerts

Pre-configured Prometheus alerts for:
- High replication lag (>300 seconds)
- High error rate (>1% of throughput)
- Service down
- DLQ growing
- Reconciliation failures

## Security

- **Secrets Management**: HashiCorp Vault for all credentials
- **Authentication**: JWT tokens with OAuth2
- **Authorization**: RBAC with Administrator, Operator, Analyst roles
- **Encryption**: TLS 1.3 for all inter-service communication
- **Audit Logging**: All API operations logged with user context
- **Security Scanning**: Bandit, Safety, pre-commit hooks

## Troubleshooting

### Common Issues

1. **Services won't start**:
   ```bash
   make clean && make up
   ```

2. **Replication lag is high**:
   - Check Kafka consumer lag: `make kafka-lag`
   - Check MinIO performance: `make minio-stats`
   - Scale Delta writer: Edit `docker/compose/docker-compose.yml`

3. **Schema evolution failed**:
   - Check Delta Lake table: `./scripts/query-deltalake.sh "DESCRIBE users"`
   - Review logs: `docker logs delta-writer`

4. **Reconciliation found discrepancies**:
   - View report: `curl http://localhost:8000/api/v1/reconciliation/reports/{id}`
   - Trigger repair: `./scripts/run-reconciliation.sh --repair`

See [Troubleshooting Guide](docs/runbooks/troubleshooting.md) for more details.

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Make changes with tests
4. Run quality checks: `make check`
5. Commit changes: `git commit -am 'Add feature'`
6. Push branch: `git push origin feature/my-feature`
7. Create Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Documentation**: [docs/](docs/)

## Roadmap

- [x] Phase 1: Real-time CDC with schema evolution
- [x] Phase 2: Error handling and reconciliation
- [x] Phase 3: Management API and security
- [ ] Phase 4: Advanced analytics features
- [ ] Phase 5: Multi-region deployment support
- [ ] Phase 6: Kubernetes operator

## Acknowledgments

- **Debezium**: MongoDB CDC connector
- **Delta Lake**: ACID transactions on object storage
- **Apache Kafka**: Event streaming platform
- **FastAPI**: Modern Python web framework
- **DuckDB**: In-process analytical database
