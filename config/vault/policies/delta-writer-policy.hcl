# Vault policy for Delta Writer service
# Allows access to MongoDB, Kafka, and MinIO secrets

path "secret/data/mongodb/*" {
  capabilities = ["read", "list"]
}

path "secret/data/kafka/*" {
  capabilities = ["read", "list"]
}

path "secret/data/minio/*" {
  capabilities = ["read", "list"]
}

path "database/creds/mongodb-reader" {
  capabilities = ["read"]
}

path "auth/token/renew-self" {
  capabilities = ["update"]
}

path "auth/token/lookup-self" {
  capabilities = ["read"]
}
