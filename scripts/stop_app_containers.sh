#!/bin/bash
#
# Stop and remove all Acquirium app containers (acquirium_app_*)
#

CONTAINERS=$(docker ps -aq --filter "name=acquirium_app_")

if [ -z "$CONTAINERS" ]; then
    echo "No acquirium_app containers found."
    exit 0
fi

echo "Found containers:"
docker ps -a --filter "name=acquirium_app_" --format "  {{.Names}} ({{.Status}})"
echo ""

echo "Stopping and removing..."
docker stop $CONTAINERS 2>/dev/null
docker rm $CONTAINERS 2>/dev/null

echo "Done."
