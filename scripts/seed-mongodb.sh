#!/bin/bash
# Seed MongoDB with test data
# Usage: ./scripts/seed-mongodb.sh [--count NUM] [--collection NAME]

set -e

# Default values
COUNT=1000
COLLECTION="users"
DATABASE="testdb"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --count)
            COUNT="$2"
            shift 2
            ;;
        --collection)
            COLLECTION="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --count NUM        Number of documents to insert (default: 1000)"
            echo "  --collection NAME  Collection name (default: users)"
            echo "  --database NAME    Database name (default: testdb)"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== Seeding MongoDB ==="
echo "Database:   $DATABASE"
echo "Collection: $COLLECTION"
echo "Count:      $COUNT"
echo

# Check if MongoDB is running
if ! docker exec mongodb mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' &>/dev/null; then
    echo "ERROR: MongoDB is not running. Start it with 'make up'"
    exit 1
fi

# Generate JavaScript code for seeding
cat > /tmp/seed-mongodb.js <<EOF
// MongoDB seed script
const db = db.getSiblingDB('$DATABASE');

// Drop existing collection
db.$COLLECTION.drop();
print('Dropped existing collection: $COLLECTION');

// Sample data generator
function generateUser() {
    const firstNames = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack'];
    const lastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'];
    const cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'];
    const states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA'];
    const tags = ['developer', 'manager', 'analyst', 'designer', 'architect', 'engineer', 'consultant', 'specialist'];

    const firstName = firstNames[Math.floor(Math.random() * firstNames.length)];
    const lastName = lastNames[Math.floor(Math.random() * lastNames.length)];
    const cityIndex = Math.floor(Math.random() * cities.length);

    return {
        name: \`\${firstName} \${lastName}\`,
        email: \`\${firstName.toLowerCase()}.\${lastName.toLowerCase()}@example.com\`,
        phone: \`+1-555-\${Math.floor(Math.random() * 10000).toString().padStart(4, '0')}\`,
        age: Math.floor(Math.random() * 63) + 18, // 18-80
        address: {
            street: \`\${Math.floor(Math.random() * 9999)} Main St\`,
            city: cities[cityIndex],
            state: states[cityIndex],
            zip: \`\${Math.floor(Math.random() * 90000) + 10000}\`,
            coordinates: {
                lat: (Math.random() * 180) - 90,
                lon: (Math.random() * 360) - 180
            }
        },
        tags: Array.from({length: Math.floor(Math.random() * 4) + 1}, () =>
            tags[Math.floor(Math.random() * tags.length)]
        ),
        is_active: Math.random() > 0.25, // 75% active
        created_at: new Date(Date.now() - Math.floor(Math.random() * 365 * 24 * 60 * 60 * 1000)),
        updated_at: new Date()
    };
}

// Batch insert
const batchSize = 1000;
const totalCount = $COUNT;
let inserted = 0;

print(\`Generating \${totalCount} documents...\`);

while (inserted < totalCount) {
    const batch = [];
    const currentBatchSize = Math.min(batchSize, totalCount - inserted);

    for (let i = 0; i < currentBatchSize; i++) {
        batch.push(generateUser());
    }

    db.$COLLECTION.insertMany(batch);
    inserted += currentBatchSize;

    if (inserted % 5000 === 0 || inserted === totalCount) {
        print(\`Inserted \${inserted} / \${totalCount} documents\`);
    }
}

// Create indexes
print('Creating indexes...');
db.$COLLECTION.createIndex({ email: 1 }, { unique: true });
db.$COLLECTION.createIndex({ 'address.city': 1 });
db.$COLLECTION.createIndex({ created_at: 1 });

// Print summary
const count = db.$COLLECTION.countDocuments();
print(\`\nSeeding complete!\`);
print(\`Collection: \${db.getName()}.$COLLECTION\`);
print(\`Total documents: \${count}\`);
EOF

# Execute seeding script
docker exec -i mongodb mongosh --quiet < /tmp/seed-mongodb.js

# Clean up
rm /tmp/seed-mongodb.js

echo
echo "=== Seeding Complete ==="
echo "You can now create a CDC pipeline with: ./scripts/create-pipeline.sh"
