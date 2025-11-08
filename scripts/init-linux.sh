#!/bin/bash

# RocketMQ Client Initialization Script for Linux
# This script initializes the RocketMQ client environment
#
# Requirements:
# 1. RocketMQ must be installed and accessible via PATH
# 2. mqadmin tool must be available
#
# What this script does:
# 1. Creates a RocketMQ topic named HelloServiceTopic
# 2. Generates a local .env file with OPENAI_API_KEY=
#
# Risks:
# 1. RocketMQ may not be installed or accessible
# 2. Insufficient permissions to create topics
# 3. Topic may already exist
# 4. File system permissions may prevent .env file creation

echo "[INFO] Starting RocketMQ client initialization for Linux..."

# Check if mqadmin is available
if ! command -v mqadmin &> /dev/null
then
    echo "[ERROR] mqadmin tool not found. Please ensure RocketMQ is installed and accessible via PATH."
    exit 1
fi

echo "[INFO] Found mqadmin tool. Proceeding with initialization..."

# Create the HelloServiceTopic
echo "[INFO] Creating HelloServiceTopic..."
if mqadmin updateTopic -t HelloServiceTopic -c DefaultCluster; then
    echo "[SUCCESS] HelloServiceTopic created successfully."
else
    echo "[WARNING] Failed to create HelloServiceTopic. It may already exist or there might be connectivity issues."
fi

# Generate .env file with OPENAI_API_KEY
echo "[INFO] Generating .env file..."
cat > .env << EOF
OPENAI_API_KEY=
EOF

if [ -f ".env" ]; then
    echo "[SUCCESS] .env file created successfully."
else
    echo "[ERROR] Failed to create .env file."
    exit 1
fi

echo "[SUCCESS] RocketMQ client initialization completed successfully!"
echo "[INFO] Please update the OPENAI_API_KEY in the .env file with your actual API key."