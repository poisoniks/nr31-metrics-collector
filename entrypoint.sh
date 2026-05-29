#!/bin/sh
set -e

# Dynamically get the group ID of the mounted docker socket
SOCKET_GID=$(stat -c '%g' /var/run/docker.sock 2>/dev/null)

if [ -n "$SOCKET_GID" ]; then
    echo "Detected docker socket GID: $SOCKET_GID"
    
    # If a group with this GID doesn't exist, create it
    if ! getent group "$SOCKET_GID" >/dev/null; then
        addgroup -g "$SOCKET_GID" docker_socket_group
    fi
    
    # Get the name of the group
    GROUP_NAME=$(getent group "$SOCKET_GID" | cut -d: -f1)
    
    # Add the node user to this group
    adduser node "$GROUP_NAME"
fi

# Switch to the node user and execute the main command
exec su-exec node "$@"
