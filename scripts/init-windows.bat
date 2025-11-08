@echo off
REM RocketMQ Client Initialization Script for Windows
REM This script initializes the RocketMQ client environment
REM
REM Requirements:
REM 1. RocketMQ must be installed and accessible via PATH
REM 2. mqadmin tool must be available
REM
REM What this script does:
REM 1. Creates a RocketMQ topic named HelloServiceTopic
REM 2. Generates a local .env file with OPENAI_API_KEY=
REM
REM Risks:
REM 1. RocketMQ may not be installed or accessible
REM 2. Insufficient permissions to create topics
REM 3. Topic may already exist
REM 4. File system permissions may prevent .env file creation

echo [INFO] Starting RocketMQ client initialization for Windows...

REM Check if mqadmin is available
where mqadmin >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] mqadmin tool not found. Please ensure RocketMQ is installed and accessible via PATH.
    exit /b 1
)

echo [INFO] Found mqadmin tool. Proceeding with initialization...

REM Create the HelloServiceTopic
echo [INFO] Creating HelloServiceTopic...
mqadmin updateTopic -t HelloServiceTopic -c DefaultCluster
if %errorlevel% equ 0 (
    echo [SUCCESS] HelloServiceTopic created successfully.
) else (
    echo [WARNING] Failed to create HelloServiceTopic. It may already exist or there might be connectivity issues.
)

REM Generate .env file with OPENAI_API_KEY
echo [INFO] Generating .env file...
(
    echo OPENAI_API_KEY=
) > .env

if exist ".env" (
    echo [SUCCESS] .env file created successfully.
) else (
    echo [ERROR] Failed to create .env file.
    exit /b 1
)

echo [SUCCESS] RocketMQ client initialization completed successfully!
echo [INFO] Please update the OPENAI_API_KEY in the .env file with your actual API key.