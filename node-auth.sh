#!/bin/bash

set -e

#Defaults
DEFAULT_TARGET_DIR="service/tools/data/cert"
DEFAULT_ENCRYPT="AES"
DEFAULT_BASE_PORT=10000
DEFAULT_IP="127.0.0.1"
DEFAULT_NUM_CLIENTS=1

#Admin Keys to generate replica keys/cert
ADMIN_PRIVATE_KEY="service/tools/data/cert/admin.key.pri"
ADMIN_PUBLIC_KEY="service/tools/data/cert/admin.key.pub"

#Parse in CLI arguements
NUM_REPLICAS=$1
NUM_CLIENTS=${2:-$DEFAULT_NUM_CLIENTS}
TARGET_DIR=${3:-$DEFAULT_TARGET_DIR}
ENCRYPT=${4:-$DEFAULT_ENCRYPT}
BASE_IP=${5:-$DEFAULT_IP}
BASE_PORT=${6:-$DEFAULT_BASE_PORT}

#Verify CLI inputs
if [ -z "$NUM_REPLICAS" ] || [ "$NUM_REPLICAS" -le 0 ] || [ -z "$NUM_CLIENTS" ] || [ "$NUM_CLIENTS" -le 0 ]; then
    echo "INVALID INPUTS"
    exit 1
fi

#Validate encryption schema
case "$ENCRYPT" in
    RSA|AES|ED25519)
        ;;
    *)
        echo "INVALID CRYPTO SCHEMA"
        exit 1
        ;;
esac

#Remove old certs and keys except for admin
echo "Cleaning up old certificates and keys..."
if [ -d "$TARGET_DIR" ]; then
    # Remove all node certificates and keys (but preserve admin keys)
    find "$TARGET_DIR" -name "node*.key.pri" -delete 2>/dev/null || true
    find "$TARGET_DIR" -name "node*.key.pub" -delete 2>/dev/null || true
    find "$TARGET_DIR" -name "cert_*.cert" -delete 2>/dev/null || true
    echo "Old certificates and keys removed"
else
    echo "Target directory does not exist, creating: $TARGET_DIR"
    mkdir -p "$TARGET_DIR"
fi

#Build service tools if they do not exist
echo "Building key generator tools..."
if ! bazel build tools:key_generator_tools; then
    echo "ERROR: Failed to build key_generator_tools"
    exit 1
fi

#Build certificate tools
echo "Building certificate tools..."
if ! bazel build tools:certificate_tools; then
    echo "ERROR: Failed to build certificate_tools"
    exit 1
fi

echo "Generating replica keys and certificates..."

# For NUM_REPLICAS=16, NUM_CLIENTS=1: 
# - Replicas: nodes 0-15 (16 total)
# - Clients: node 16 (1 total)
# - Total: 17 nodes (0-16)

#Generate keys and cert for each replica and client
for i in $(seq 0 $((NUM_REPLICAS + NUM_CLIENTS - 1))); do
    NODE_NAME="node$i"
    PRIVATE_KEY="$TARGET_DIR/${NODE_NAME}.key.pri"
    PUBLIC_KEY="$TARGET_DIR/${NODE_NAME}.key.pub"
    CERTIFICATE="$TARGET_DIR/cert_${i}.cert"

    #Set IP and base port for this node
    REPLICA_IP="$BASE_IP"

    #Port number == Node number
    REPLICA_PORT=$((BASE_PORT + i))

    echo "Processing node $i ($NODE_NAME)..."
    
    #Generate the keys if they don't already exist
    if [ ! -f "$PRIVATE_KEY" ] || [ ! -f "$PUBLIC_KEY" ]; then
        echo "Generating keys for $NODE_NAME..."
        if bazel-bin/tools/key_generator_tools "$TARGET_DIR/$NODE_NAME" "$ENCRYPT"; then
            echo "Keys successfully generated: $PRIVATE_KEY, $PUBLIC_KEY"
        else
            echo "Failed to generate key for $NODE_NAME"
            continue
        fi
    else
        echo "Keys already exist for $NODE_NAME"
    fi

    #Generate the certificate if it doesn't already exist
    if [ ! -f "$CERTIFICATE" ]; then
        if (( i >= NUM_REPLICAS )); then
            echo "Generating client certificate for $i ($NODE_NAME)..."
            if bazel-bin/tools/certificate_tools "$TARGET_DIR" "$ADMIN_PRIVATE_KEY" "$ADMIN_PUBLIC_KEY" "$PUBLIC_KEY" "$i" "$REPLICA_IP" "$REPLICA_PORT" "client"; then
                echo "Client certificate generated: $CERTIFICATE"
            else
                echo "ERROR: Failed to generate client certificate for $NODE_NAME"
                continue
            fi
        else
            echo "Generating replica certificate for $NODE_NAME..."
            if bazel-bin/tools/certificate_tools "$TARGET_DIR" "$ADMIN_PRIVATE_KEY" "$ADMIN_PUBLIC_KEY" "$PUBLIC_KEY" "$i" "$REPLICA_IP" "$REPLICA_PORT" "replica"; then
                echo "Replica certificate generated: $CERTIFICATE"
            else
                echo "ERROR: Failed to generate replica certificate for $NODE_NAME"
                continue
            fi
        fi
    else
        echo "Certificate already exists for $NODE_NAME"
    fi

    # Determine node type for output
    if (( i >= NUM_REPLICAS )); then
        NODE_TYPE="client"
    else
        NODE_TYPE="replica"
    fi

    echo "$NODE_TYPE $i: IP=$REPLICA_IP, Port=$REPLICA_PORT"
    echo ""
done

echo "Generation complete!"
echo "Generated $NUM_REPLICAS replica certificates (node0 to node$((NUM_REPLICAS-1)))"
echo "Generated $NUM_CLIENTS client certificates (node$NUM_REPLICAS to node$((NUM_REPLICAS + NUM_CLIENTS - 1)))"