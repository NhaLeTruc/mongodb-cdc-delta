#!/bin/bash
# Create CDC pipeline via Kafka Connect
# Usage: ./scripts/create-pipeline.sh [--collection NAME] [--database NAME]

set -e

# Default values
COLLECTION="users"
DATABASE="testdb"
CONNECTOR_NAME="mongodb-cdc-${DATABASE}-${COLLECTION}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --collection)
            COLLECTION="$2"
            CONNECTOR_NAME="mongodb-cdc-${DATABASE}-${COLLECTION}"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            CONNECTOR_NAME="mongodb-cdc-${DATABASE}-${COLLECTION}"
            shift 2
            ;;
        --name)
            CONNECTOR_NAME="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --collection NAME  Collection name (default: users)"
            echo "  --database NAME    Database name (default: testdb)"
            echo "  --name NAME        Connector name (default: mongodb-cdc-{database}-{collection})"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== Creating CDC Pipeline ==="
echo "Database:   $DATABASE"
echo "Collection: $COLLECTION"
echo "Connector:  $CONNECTOR_NAME"
echo

# Check if Kafka Connect is running
if ! curl -sf http://localhost:8083/ &>/dev/null; then
    echo "ERROR: Kafka Connect is not running. Start it with 'make up'"
    exit 1
fi

# Check if connector already exists
if curl -sf "http://localhost:8083/connectors/$CONNECTOR_NAME" &>/dev/null; then
    echo "Connector '$CONNECTOR_NAME' already exists. Deleting..."
    curl -X DELETE "http://localhost:8083/connectors/$CONNECTOR_NAME"
    sleep 2
fi

# Create connector configuration
cat > /tmp/connector-config.json <<EOF
{
  "name": "$CONNECTOR_NAME",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://admin:admin123@mongodb:27017/?replicaSet=rs0",
    "mongodb.connection.mode": "replica_set",
    "topic.prefix": "mongodb",
    "collection.include.list": "$DATABASE.$COLLECTION",

    "snapshot.mode": "initial",
    "snapshot.fetch.size": 1000,

    "poll.interval.ms": 100,
    "max.batch.size": 2048,
    "max.queue.size": 8192,
    "tasks.max": 1,

    "tombstones.on.delete": true,
    "capture.mode": "change_streams_update_full",

    "heartbeat.interval.ms": 5000,
    "heartbeat.topics.prefix": "__debezium-heartbeat",

    "schema.name.adjustment.mode": "avro",
    "provide.transaction.metadata": true,

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
    "transforms.unwrap.drop.tombstones": false,
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms,source.db,source.collection"
  }
}
EOF

# Deploy connector
echo "Deploying connector to Kafka Connect..."
response=$(curl -s -X POST -H "Content-Type: application/json" \
    --data @/tmp/connector-config.json \
    http://localhost:8083/connectors)

# Clean up
rm /tmp/connector-config.json

# Check if deployment succeeded
if echo "$response" | grep -q "error"; then
    echo "ERROR: Failed to create connector"
    echo "$response" | python3 -m json.tool
    exit 1
fi

echo "Connector created successfully!"
echo

# Wait for connector to be running
echo "Waiting for connector to start..."
for i in {1..30}; do
    status=$(curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/status" | \
        python3 -c "import sys, json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")

    if [ "$status" = "RUNNING" ]; then
        echo "Connector is RUNNING!"
        break
    fi

    if [ $i -eq 30 ]; then
        echo "WARNING: Connector did not start within 30 seconds"
        echo "Check status with: curl http://localhost:8083/connectors/$CONNECTOR_NAME/status"
    fi

    sleep 1
done

echo
echo "=== Pipeline Created Successfully ==="
echo
echo "Topic: mongodb.$DATABASE.$COLLECTION"
echo
echo "Check connector status:"
echo "  curl http://localhost:8083/connectors/$CONNECTOR_NAME/status | python3 -m json.tool"
echo
echo "View Kafka topics:"
echo "  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list"
echo
echo "Consume change events:"
echo "  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic mongodb.$DATABASE.$COLLECTION --from-beginning"
echo
