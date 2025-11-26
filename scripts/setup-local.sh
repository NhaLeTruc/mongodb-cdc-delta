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
    local max_attempts=30
    local attempt=0

    print_info "Waiting for $service_name to be ready..."

    while [ $attempt -lt $max_attempts ]; do
        if eval "$check_command" &>/dev/null; then
            print_info "$service_name is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    print_error "$service_name failed to become ready after $max_attempts attempts"
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

# Wait for MongoDB
wait_for_service "MongoDB" "docker exec mongodb mongosh --quiet --eval 'db.adminCommand({ ping: 1 })'"

# Wait for Kafka
wait_for_service "Kafka" "docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092"

# Wait for MinIO
wait_for_service "MinIO" "curl -sf http://localhost:9000/minio/health/live"

# Wait for PostgreSQL
wait_for_service "PostgreSQL" "docker exec postgres pg_isready -U cdc_admin"

# Wait for Kafka Connect
wait_for_service "Kafka Connect" "curl -sf http://localhost:8083/"

# Wait for Vault
wait_for_service "Vault" "curl -sf http://localhost:8200/v1/sys/health"

# Wait for Prometheus
wait_for_service "Prometheus" "curl -sf http://localhost:9090/-/healthy"

# Wait for Grafana
wait_for_service "Grafana" "curl -sf http://localhost:3000/api/health"

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
