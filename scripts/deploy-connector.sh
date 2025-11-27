#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONNECTOR_CONFIG="$PROJECT_ROOT/config/kafka-connect/debezium-mongodb.json"
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
MAX_RETRIES=30
RETRY_DELAY=5

echo "===== Debezium MongoDB Connector Deployment ====="
echo "Kafka Connect URL: $KAFKA_CONNECT_URL"
echo "Connector Config: $CONNECTOR_CONFIG"
echo ""

if [ ! -f "$CONNECTOR_CONFIG" ]; then
    echo "ERROR: Connector configuration file not found: $CONNECTOR_CONFIG"
    exit 1
fi

wait_for_kafka_connect() {
    echo "Waiting for Kafka Connect to be ready..."
    local retries=0

    while [ $retries -lt $MAX_RETRIES ]; do
        if curl -s -f "$KAFKA_CONNECT_URL/" > /dev/null 2>&1; then
            echo "Kafka Connect is ready!"
            return 0
        fi

        retries=$((retries + 1))
        echo "Attempt $retries/$MAX_RETRIES: Kafka Connect not ready yet, waiting ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done

    echo "ERROR: Kafka Connect did not become ready after $MAX_RETRIES attempts"
    return 1
}

check_connector_exists() {
    local connector_name="$1"

    if curl -s -f "$KAFKA_CONNECT_URL/connectors/$connector_name" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

delete_connector() {
    local connector_name="$1"

    echo "Deleting existing connector: $connector_name"
    if curl -s -X DELETE "$KAFKA_CONNECT_URL/connectors/$connector_name" > /dev/null 2>&1; then
        echo "Connector deleted successfully"
        sleep 2
    else
        echo "WARNING: Failed to delete connector (may not exist)"
    fi
}

deploy_connector() {
    local config_file="$1"
    local connector_name=$(jq -r '.name' "$config_file")

    echo "Deploying connector: $connector_name"

    if check_connector_exists "$connector_name"; then
        echo "Connector already exists"
        read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            delete_connector "$connector_name"
        else
            echo "Skipping deployment"
            return 0
        fi
    fi

    echo "Creating connector..."
    local response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        --data "@$config_file" \
        "$KAFKA_CONNECT_URL/connectors")

    if echo "$response" | jq -e '.name' > /dev/null 2>&1; then
        echo "Connector created successfully!"
        echo "$response" | jq '.'
    else
        echo "ERROR: Failed to create connector"
        echo "$response" | jq '.'
        return 1
    fi
}

verify_connector() {
    local connector_name="$1"
    local retries=0
    local max_verify_retries=10

    echo ""
    echo "Verifying connector status..."

    while [ $retries -lt $max_verify_retries ]; do
        local status=$(curl -s "$KAFKA_CONNECT_URL/connectors/$connector_name/status")
        local connector_state=$(echo "$status" | jq -r '.connector.state')
        local task_state=$(echo "$status" | jq -r '.tasks[0].state')

        echo "Connector State: $connector_state"
        echo "Task State: $task_state"

        if [ "$connector_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
            echo ""
            echo "SUCCESS: Connector is running!"
            echo "$status" | jq '.'
            return 0
        elif [ "$connector_state" = "FAILED" ] || [ "$task_state" = "FAILED" ]; then
            echo ""
            echo "ERROR: Connector failed!"
            echo "$status" | jq '.'
            return 1
        fi

        retries=$((retries + 1))
        echo "Waiting for connector to start... ($retries/$max_verify_retries)"
        sleep 3
    done

    echo "WARNING: Connector status verification timed out"
    return 1
}

list_connectors() {
    echo ""
    echo "Current connectors:"
    curl -s "$KAFKA_CONNECT_URL/connectors" | jq '.'
}

main() {
    if ! wait_for_kafka_connect; then
        exit 1
    fi

    if ! deploy_connector "$CONNECTOR_CONFIG"; then
        exit 1
    fi

    local connector_name=$(jq -r '.name' "$CONNECTOR_CONFIG")
    verify_connector "$connector_name"

    list_connectors

    echo ""
    echo "===== Deployment Complete ====="
}

main "$@"
