# Kafka Connect with Debezium MongoDB Connector
FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# Install Debezium MongoDB Connector
# This is the critical connector for MongoDB CDC (Change Data Capture)
RUN confluent-hub install --no-prompt debezium/debezium-connector-mongodb:2.5.0

# Install JDBC Sink Connector (optional, for metadata storage)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4

# Create directories for custom connectors and transforms
RUN mkdir -p /usr/share/confluent-hub-components /opt/transforms

# Set proper permissions
RUN chown -R appuser:appuser /usr/share/confluent-hub-components /opt/transforms

USER appuser

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=60s \
  CMD curl -f http://localhost:8083/ || exit 1

# The CONNECT_PLUGIN_PATH environment variable in docker-compose.yml includes:
# /usr/share/java,/usr/share/confluent-hub-components,/opt/transforms
# So all installed connectors will be discovered automatically

EXPOSE 8083
