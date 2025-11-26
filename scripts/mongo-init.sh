#!/bin/bash
# MongoDB Replica Set Initialization Script
# This script initializes a single-node replica set for CDC

set -e

echo "Waiting for MongoDB to start..."
sleep 10

echo "Initiating replica set..."
mongosh <<EOF
try {
  rs.status();
  print("Replica set already initialized");
} catch(e) {
  print("Initializing replica set...");
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongodb:27017" }
    ]
  });
  print("Replica set initialized successfully");
}

// Wait for replica set to be ready
var attempt = 0;
while (attempt < 30) {
  try {
    var status = rs.status();
    if (status.members[0].stateStr === "PRIMARY") {
      print("Replica set is ready");
      break;
    }
  } catch(e) {}
  attempt++;
  sleep(1000);
}

// Create test database and collection
use testdb;
db.createCollection("users");
db.users.createIndex({ email: 1 }, { unique: true });
print("Test database and collection created");
EOF

echo "MongoDB initialization complete"
