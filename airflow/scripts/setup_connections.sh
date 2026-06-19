#!/bin/bash
# Creates Airflow connections from .env values
# Run once after `docker compose up -d`

set -e

if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "ERROR: .env file not found. Copy .env.example to .env first."
    exit 1
fi

CONTAINER="footballflow_airflow_scheduler"

echo "Creating Snowflake connection..."
docker exec -it $CONTAINER airflow connections add "snowflake_footballflow" \
    --conn-type     "snowflake" \
    --conn-host     "${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com" \
    --conn-login    "${SNOWFLAKE_USER}" \
    --conn-password "${SNOWFLAKE_PASSWORD}" \
    --conn-schema   "RAW" \
    --conn-extra    "{
        \"account\": \"${SNOWFLAKE_ACCOUNT}\",
        \"warehouse\": \"${SNOWFLAKE_WAREHOUSE}\",
        \"database\": \"${SNOWFLAKE_DATABASE}\",
        \"role\": \"${SNOWFLAKE_ROLE}\",
        \"authenticator\": \"snowflake\"
    }"

echo "  ✓ snowflake_footballflow"

echo "Creating AWS connection..."
docker exec -it $CONTAINER airflow connections add "aws_footballflow" \
    --conn-type     "aws" \
    --conn-login    "${AWS_ACCESS_KEY_ID}" \
    --conn-password "${AWS_SECRET_ACCESS_KEY}" \
    --conn-extra    "{\"region_name\": \"${AWS_DEFAULT_REGION}\"}"

echo "  ✓ aws_footballflow"

echo ""
docker exec -it $CONTAINER airflow connections list
