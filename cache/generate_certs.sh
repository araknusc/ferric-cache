#!/bin/bash

# Generate self-signed certificates for local TLS testing

CERT_DIR="./certs"
DAYS=365
KEY_SIZE=2048

echo "Creating certificate directory..."
mkdir -p $CERT_DIR

echo "Generating self-signed certificate and key..."
openssl req -x509 \
    -newkey rsa:$KEY_SIZE \
    -keyout $CERT_DIR/server.key \
    -out $CERT_DIR/server.crt \
    -days $DAYS \
    -nodes \
    -subj "/C=US/ST=State/L=City/O=ferric-cache/CN=localhost"

echo "Certificates generated successfully!"
echo "  Certificate: $CERT_DIR/server.crt"
echo "  Private Key: $CERT_DIR/server.key"
echo ""
echo "To start the server with TLS:"
echo "  cargo run --release -- --config tls_config.json"