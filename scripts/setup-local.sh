#!/bin/bash
# Initialize local development environment
# This script sets up Vault, MinIO buckets, and waits for all services

set -e

echo "=== MongoDB CDC to Delta Lake - Local Environment Setup ==="
echo

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local check_command=$2
    local max_attempts=${3:-30}
    local attempt=0

    print_info "Waiting for $service_name to be ready..."

    while [ $attempt -lt $max_attempts ]; do
        if eval "$check_command" &>/dev/null; then
            print_info "$service_name is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done

    echo
    print_error "$service_name failed to become ready after $((max_attempts * 2)) seconds"
    return 1
}

# Function to check Docker Compose health status
wait_for_healthy() {
    local service_name=$1
    local max_attempts=${2:-60}
    local attempt=0

    print_info "Waiting for $service_name health check..."

    while [ $attempt -lt $max_attempts ]; do
        health_status=$(docker compose ps --format json | jq -r ".[] | select(.Service == \"$service_name\") | .Health" 2>/dev/null)

        if [ "$health_status" == "healthy" ]; then
            print_info "$service_name is healthy!"
            return 0
        fi

        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done

    echo
    print_error "$service_name did not become healthy after $((max_attempts * 2)) seconds"
    return 1
}

# Check if Docker is running
if ! docker info &>/dev/null; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    print_error "docker-compose.yml not found. Are you in the project root?"
    exit 1
fi

print_info "Starting all services..."
docker compose up -d

echo

# Wait for core services using Docker health checks
wait_for_healthy "mongodb" 60
wait_for_healthy "zookeeper" 30
wait_for_healthy "kafka" 60
wait_for_healthy "minio" 30
wait_for_healthy "postgres" 30

# Wait for dependent services
wait_for_healthy "kafka-connect" 90
wait_for_healthy "prometheus" 30
wait_for_healthy "grafana" 30

# Verify Jaeger if present
if docker compose ps | grep -q jaeger; then
    wait_for_service "Jaeger" "curl -sf http://localhost:14269/" 30
fi

# Initialize MongoDB replica set if needed
print_info "Verifying MongoDB replica set..."
docker exec cdc-mongodb mongosh --quiet --eval "
try {
    rs.status();
    print('Replica set already initialized');
} catch(e) {
    rs.initiate({
        _id: 'rs0',
        members: [{_id: 0, host: 'mongodb:27017'}]
    });
    print('Replica set initialized');
}" || print_warn "MongoDB replica set check failed"

echo
print_info "All services are ready!"

echo
print_info "Initializing Vault..."

# Configure Vault (in dev mode, already initialized)
export VAULT_ADDR='http://localhost:8200'
export VAULT_TOKEN='root-token'

# Create secret mount if not exists
docker exec vault vault secrets enable -path=secret kv-v2 2>/dev/null || print_warn "Secret mount already exists"

# Store sample secrets
print_info "Storing sample secrets in Vault..."

docker exec -e VAULT_TOKEN=root-token vault vault kv put secret/mongodb/credentials \
    username=admin \
    password=admin123 \
    connection_string="mongodb://admin:admin123@mongodb:27017/?replicaSet=rs0"

docker exec -e VAULT_TOKEN=root-token vault vault kv put secret/kafka/credentials \
    bootstrap_servers="kafka:9092"

docker exec -e VAULT_TOKEN=root-token vault vault kv put secret/minio/credentials \
    access_key=minioadmin \
    secret_key=minioadmin123 \
    endpoint="http://minio:9000"

print_info "Vault initialization complete"

echo
print_info "Creating MinIO buckets..."

# MinIO buckets are created by minio-init container, verify they exist
if docker exec minio-init mc ls myminio/lakehouse &>/dev/null; then
    print_info "MinIO buckets verified"
else
    print_warn "MinIO bucket creation may have failed, check logs"
fi

echo
print_info "=== Environment Setup Complete ==="
echo
echo "Services available at:"
echo "  - MongoDB:          mongodb://localhost:27017"
echo "  - Kafka:            localhost:29092"
echo "  - Kafka Connect:    http://localhost:8083"
echo "  - MinIO Console:    http://localhost:9001 (minioadmin/minioadmin123)"
echo "  - PostgreSQL:       localhost:5432 (cdc_admin/cdc_password)"
echo "  - Prometheus:       http://localhost:9090"
echo "  - Grafana:          http://localhost:3000 (admin/admin)"
echo "  - Jaeger:           http://localhost:16686"
echo "  - Vault:            http://localhost:8200 (root-token)"
echo
echo "Next steps:"
echo "  1. Run 'make seed' to populate test data"
echo "  2. Run './scripts/create-pipeline.sh' to create a CDC pipeline"
echo "  3. Run 'make test' to run the test suite"
echo

print_info "Environment is ready for development!"
