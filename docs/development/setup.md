# Local Development Setup Guide

This guide walks you through setting up the MongoDB CDC to Delta Lake pipeline for local development and testing.

## Prerequisites

### Required Software

- **Docker Desktop** (v20.10+) or **Docker Engine** + **Docker Compose** (v2.0+)
- **Git** (v2.30+)
- **Python** (v3.10+) for running scripts and tests
- **Make** (for using Makefile commands)
- **curl** and **jq** (for health check scripts)

### System Requirements

- **RAM**: Minimum 8GB, Recommended 16GB
- **Disk Space**: Minimum 20GB free
- **CPU**: Minimum 4 cores, Recommended 8 cores
- **OS**: Linux, macOS, or Windows with WSL2

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/mongodb-cdc-delta.git
cd mongodb-cdc-delta
```

### 2. Configure Environment Variables

Copy the example environment file and customize it:

```bash
cp docker/compose/.env.example docker/compose/.env
```

Edit `docker/compose/.env` and update values as needed. The defaults work for local development.

**Key Variables**:
```env
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=admin123
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### 3. Start All Services

Use the setup script to start all services and wait for them to be healthy:

```bash
./scripts/setup-local.sh
```

This script will:
- Start all Docker containers
- Wait for all health checks to pass
- Initialize MongoDB replica set
- Create MinIO buckets
- Configure Vault (if applicable)

**Expected Duration**: 2-5 minutes

### 4. Verify Services

Check that all services are running:

```bash
docker compose ps
```

All services should show `healthy` status.

Access service UIs:
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jaeger**: http://localhost:16686
- **Kafka Connect**: http://localhost:8083/connectors

### 5. Seed Test Data (Optional)

Populate MongoDB with test data:

```bash
make seed
```

Or manually:

```bash
python tests/load/data_generators/mongodb_seeder.py --count 10000
```

### 6. Create a CDC Pipeline

Create a Debezium connector to capture MongoDB changes:

```bash
./scripts/create-pipeline.sh users
```

This creates a pipeline for the `users` collection.

### 7. Run Tests

Run the full test suite:

```bash
make test-local
```

Or run specific test types:

```bash
# Unit tests only
pytest tests/unit

# Integration tests (requires Docker)
pytest tests/integration

# E2E tests
pytest tests/e2e
```

## Detailed Setup Instructions

### Docker Compose Architecture

The local environment includes the following services:

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| MongoDB | cdc-mongodb | 27017 | Source database with replica set |
| Zookeeper | cdc-zookeeper | 2181 | Kafka coordination |
| Kafka | cdc-kafka | 9092, 29092 | Message broker for CDC events |
| Kafka Connect | cdc-kafka-connect | 8083 | Debezium connectors |
| MinIO | cdc-minio | 9000, 9001 | S3-compatible Delta Lake storage |
| PostgreSQL | cdc-postgres | 5432 | Metadata and checkpoints |
| Prometheus | cdc-prometheus | 9090 | Metrics collection |
| Grafana | cdc-grafana | 3000 | Metrics visualization |
| Jaeger | cdc-jaeger | 16686 | Distributed tracing |
| Delta Writer | cdc-delta-writer | - | Kafka → Delta Lake consumer |

### Service Dependencies

```
MongoDB (replica set) ← Kafka Connect → Kafka → Delta Writer → MinIO (Delta Lake)
                                          ↓
                                      PostgreSQL (checkpoints)
```

### Health Checks

All services have health checks configured. The setup script waits for:

1. **MongoDB**: Replica set initialized and responding to ping
2. **Zookeeper**: Port 2181 accessible
3. **Kafka**: Broker API responding
4. **MinIO**: Health endpoint returns 200
5. **PostgreSQL**: `pg_isready` check passes
6. **Kafka Connect**: Connectors endpoint accessible
7. **Prometheus**: Healthy endpoint returns 200
8. **Grafana**: Health API returns 200

### Volume Persistence

Data is persisted in Docker volumes:

- `mongodb_data`: MongoDB database files
- `kafka_data`: Kafka logs and topics
- `minio_data`: Delta Lake files
- `postgres_data`: Metadata database
- `prometheus_data`: Metrics data
- `grafana_data`: Dashboards and settings

To reset the environment:

```bash
make clean  # Stops containers and removes volumes
```

## Development Workflow

### 1. Make Code Changes

Edit files in your preferred IDE:
- `delta-writer/src/` - Delta Lake writer service
- `api/src/` - FastAPI management service
- `reconciliation/src/` - Reconciliation engine
- `shared/` - Shared utilities

### 2. Rebuild Services

After code changes, rebuild the affected service:

```bash
# Rebuild and restart delta-writer
docker compose up -d --build delta-writer

# View logs
docker compose logs -f delta-writer
```

### 3. Run Tests

Run tests to verify your changes:

```bash
# Run unit tests (fast)
pytest tests/unit -v

# Run integration tests (slower, requires Docker)
pytest tests/integration -v

# Run specific test file
pytest tests/unit/test_retry.py -v
```

### 4. Check Code Quality

Before committing:

```bash
# Format code
make format

# Run linters
make lint

# Run type checks
make typecheck
```

### 5. Commit Changes

```bash
git add .
git commit -m "Your commit message"
```

Pre-commit hooks will run automatically.

## Common Commands

### Make Targets

```bash
make help           # Show all available commands
make up             # Start all services
make down           # Stop all services
make restart        # Restart all services
make logs           # View logs from all services
make test           # Run full test suite
make test-local     # Run tests with local Docker env
make lint           # Run linters (Black, Ruff, mypy)
make format         # Format code
make clean          # Stop and remove all containers and volumes
make seed           # Seed MongoDB with test data
```

### Docker Compose Commands

```bash
# Start specific service
docker compose up -d mongodb kafka

# View logs
docker compose logs -f delta-writer

# Check service status
docker compose ps

# Execute command in container
docker compose exec mongodb mongosh

# Restart service
docker compose restart delta-writer

# Stop all services
docker compose down

# Stop and remove volumes
docker compose down -v
```

### Debugging

**View service logs**:
```bash
docker compose logs -f <service-name>
```

**Check service health**:
```bash
docker compose ps
docker inspect --format='{{.State.Health.Status}}' cdc-mongodb
```

**Execute commands in containers**:
```bash
# MongoDB shell
docker exec -it cdc-mongodb mongosh

# Kafka topics
docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092

# MinIO client
docker exec cdc-minio-init mc ls myminio

# PostgreSQL shell
docker exec -it cdc-postgres psql -U postgres -d cdc_metadata
```

**Check Kafka topics**:
```bash
docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic mongodb.cdcdb.users --from-beginning --max-messages 10
```

**View Delta Lake files**:
```bash
docker exec cdc-minio-init mc ls myminio/lakehouse
```

## Troubleshooting

### Services Not Starting

**Problem**: Services fail to start or health checks timeout

**Solutions**:
1. Check Docker resources (increase RAM to 8GB+)
2. Ensure no port conflicts: `lsof -i :9092` (macOS/Linux)
3. View logs: `docker compose logs <service>`
4. Restart Docker Desktop
5. Clean and restart: `make clean && make up`

### MongoDB Replica Set Not Initialized

**Problem**: MongoDB shows "not master" errors

**Solution**:
```bash
docker exec cdc-mongodb mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'mongodb:27017'}]})"
```

### Kafka Connect Cannot Reach MongoDB

**Problem**: Debezium connector fails to connect

**Solutions**:
1. Verify MongoDB replica set: `docker exec cdc-mongodb mongosh --eval "rs.status()"`
2. Check network: `docker network inspect cdc-network`
3. Verify connector config: `curl http://localhost:8083/connectors/<connector-name>/config`

### MinIO Connection Failures

**Problem**: Delta Writer cannot connect to MinIO

**Solutions**:
1. Check MinIO health: `curl http://localhost:9000/minio/health/live`
2. Verify buckets exist: `docker exec cdc-minio-init mc ls myminio`
3. Check credentials in `.env` file

### Tests Failing

**Problem**: Tests fail with "connection refused" or timeout errors

**Solutions**:
1. Ensure all services are healthy: `docker compose ps`
2. Wait for services to fully start (2-5 minutes after `docker compose up`)
3. Check test requirements installed: `pip install -r tests/requirements.txt`
4. Run with verbose output: `pytest -vv -s`

### High Resource Usage

**Problem**: Docker using too much CPU/RAM

**Solutions**:
1. Reduce number of running services (comment out in docker-compose.yml)
2. Limit resource usage in docker-compose.yml:
   ```yaml
   deploy:
     resources:
       limits:
         memory: 1G
   ```
3. Increase Docker Desktop resources in Settings

### Port Conflicts

**Problem**: "Port already in use" errors

**Solutions**:
1. Find conflicting process: `lsof -i :<port>` (macOS/Linux) or `netstat -ano | findstr :<port>` (Windows)
2. Stop conflicting service or change port in `docker-compose.yml`

## Advanced Configuration

### Custom MongoDB Initialization

Add custom init scripts in `docker/mongodb/init-scripts/`:

```javascript
// 00-create-users.js
db.createUser({
  user: "app_user",
  pwd: "app_password",
  roles: [{role: "readWrite", db: "cdcdb"}]
});
```

Mount in docker-compose.yml:
```yaml
volumes:
  - ./mongodb/init-scripts:/docker-entrypoint-initdb.d
```

### Custom Kafka Topics

Create topics manually:

```bash
docker exec cdc-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic custom.topic \
  --partitions 3 \
  --replication-factor 1
```

### Enabling Debug Logging

Set in `.env`:
```env
LOG_LEVEL=DEBUG
DEBUG=true
```

Or for specific services:
```bash
docker compose exec delta-writer env LOG_LEVEL=DEBUG python -m delta_writer.main
```

## Next Steps

- Read [Testing Guide](testing.md) for detailed testing instructions
- Review [Architecture Documentation](../architecture/overview.md)
- Check [API Documentation](../api/openapi.yaml)
- See [Troubleshooting Guide](../runbooks/troubleshooting.md)

## Getting Help

- **Issues**: https://github.com/your-org/mongodb-cdc-delta/issues
- **Documentation**: https://docs.your-org.com/mongodb-cdc-delta
- **Slack**: #cdc-pipeline-support
