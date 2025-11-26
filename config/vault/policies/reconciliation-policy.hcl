# Vault policy for Reconciliation service
# Allows read-only access to MongoDB and Delta Lake secrets

path "secret/data/mongodb/*" {
  capabilities = ["read", "list"]
}

path "secret/data/minio/*" {
  capabilities = ["read", "list"]
}

path "secret/data/postgres/*" {
  capabilities = ["read", "list"]
}

path "database/creds/mongodb-reader" {
  capabilities = ["read"]
}

path "database/creds/postgres-reconciliation" {
  capabilities = ["read"]
}

path "auth/token/renew-self" {
  capabilities = ["update"]
}

path "auth/token/lookup-self" {
  capabilities = ["read"]
}
