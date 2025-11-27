#!/bin/bash
# Teardown script for MongoDB CDC to Delta Lake Pipeline
# Cleanly shuts down all services and optionally removes volumes

set -e

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

echo "=== MongoDB CDC to Delta Lake - Environment Teardown ==="
echo

# Parse command line arguments
REMOVE_VOLUMES=false
REMOVE_IMAGES=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --volumes|-v)
            REMOVE_VOLUMES=true
            shift
            ;;
        --images|-i)
            REMOVE_IMAGES=true
            shift
            ;;
        --force|-f)
            FORCE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --volumes, -v    Remove all volumes (destroys data)"
            echo "  --images, -i     Remove Docker images"
            echo "  --force, -f      Skip confirmation prompts"
            echo "  --help, -h       Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if Docker is running
if ! docker info &>/dev/null; then
    print_error "Docker is not running"
    exit 1
fi

# Change to docker compose directory
cd "$(dirname "$0")/../docker/compose" || exit 1

# Confirmation prompts
if [ "$FORCE" != "true" ]; then
    if [ "$REMOVE_VOLUMES" = "true" ]; then
        print_warn "This will PERMANENTLY DELETE all data volumes!"
        read -p "Are you sure? (yes/no): " confirm
        if [ "$confirm" != "yes" ]; then
            print_info "Teardown cancelled"
            exit 0
        fi
    fi
fi

print_info "Stopping all services..."

# Stop all containers gracefully
if docker compose ps --services | grep -q .; then
    # Get list of running services
    services=$(docker compose ps --services)

    if [ -n "$services" ]; then
        print_info "Gracefully stopping services..."

        # Stop delta-writer first to allow checkpoint commit
        if echo "$services" | grep -q "delta-writer"; then
            print_info "Stopping Delta Writer (allowing checkpoint flush)..."
            docker compose stop -t 30 delta-writer
        fi

        # Stop Kafka Connect
        if echo "$services" | grep -q "kafka-connect"; then
            print_info "Stopping Kafka Connect..."
            docker compose stop -t 20 kafka-connect
        fi

        # Stop remaining services
        print_info "Stopping remaining services..."
        docker compose stop

        print_info "All services stopped"
    else
        print_info "No running services found"
    fi
else
    print_info "No services are currently running"
fi

# Remove containers
print_info "Removing containers..."
docker compose down --remove-orphans

# Remove volumes if requested
if [ "$REMOVE_VOLUMES" = "true" ]; then
    print_warn "Removing all data volumes..."

    # List volumes before removal
    volumes=$(docker volume ls --filter "name=mongodb-cdc-delta" -q)

    if [ -n "$volumes" ]; then
        echo "Volumes to be removed:"
        echo "$volumes" | sed 's/^/  - /'

        docker volume rm $volumes 2>/dev/null || print_warn "Some volumes could not be removed (may be in use)"

        print_info "Volumes removed"
    else
        print_info "No project volumes found"
    fi
else
    print_info "Volumes preserved (use --volumes to remove)"
fi

# Remove images if requested
if [ "$REMOVE_IMAGES" = "true" ]; then
    print_warn "Removing Docker images..."

    # Remove custom-built images
    images=$(docker images --filter "reference=mongodb-cdc-delta*" -q)

    if [ -n "$images" ]; then
        docker rmi $images 2>/dev/null || print_warn "Some images could not be removed"
        print_info "Custom images removed"
    else
        print_info "No custom images found"
    fi

    # Optionally remove downloaded images
    if [ "$FORCE" = "true" ]; then
        print_info "Removing downloaded images..."
        docker compose down --rmi all 2>/dev/null || print_warn "Could not remove all images"
    fi
else
    print_info "Docker images preserved (use --images to remove)"
fi

# Clean up orphaned networks
print_info "Cleaning up networks..."
docker network prune -f 2>/dev/null || true

# Show remaining resources
echo
print_info "Remaining Docker resources:"
echo

# Show volumes
volumes=$(docker volume ls --filter "name=mongodb-cdc-delta" -q)
if [ -n "$volumes" ]; then
    echo "Volumes:"
    echo "$volumes" | sed 's/^/  - /'
else
    echo "Volumes: None"
fi

# Show images
images=$(docker images --filter "reference=mongodb-cdc-delta*" --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}" | tail -n +2)
if [ -n "$images" ]; then
    echo
    echo "Images:"
    echo "$images" | sed 's/^/  - /'
else
    echo
    echo "Images: None"
fi

# Clean up local files (optional)
echo
read -p "Remove local cache files (.pytest_cache, __pycache__, etc.)? (y/n): " clean_cache

if [ "$clean_cache" = "y" ] || [ "$clean_cache" = "yes" ]; then
    print_info "Cleaning local cache files..."
    cd "$(dirname "$0")/.." || exit 1

    # Clean Python caches
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true

    # Clean coverage reports
    rm -rf htmlcov .coverage 2>/dev/null || true

    # Clean checkpoints
    rm -rf delta-writer/checkpoints/* 2>/dev/null || true

    print_info "Local cache files cleaned"
fi

echo
print_info "=== Teardown Complete ==="
echo

# Show summary
echo "Summary:"
echo "  - Containers: Stopped and removed"
if [ "$REMOVE_VOLUMES" = "true" ]; then
    echo "  - Volumes: Removed (data deleted)"
else
    echo "  - Volumes: Preserved"
fi
if [ "$REMOVE_IMAGES" = "true" ]; then
    echo "  - Images: Removed"
else
    echo "  - Images: Preserved"
fi

echo
echo "To start the environment again:"
echo "  make up"
echo "  # or"
echo "  ./scripts/setup-local.sh"
echo
