#!/bin/bash
# Quick script to run MISO wind forecast scraper

# Set up environment
export PYTHONPATH=/Users/mark.johnson/Desktop/source/repos/mark.johnson/claude_scrape_agent_testbed:$PYTHONPATH
export S3_BUCKET=scraper-testing
export AWS_PROFILE=localstack
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0

# Activate virtual environment
source venv/bin/activate

# Run scraper
python3 sourcing/scraping/miso/scraper_miso_wind_forecast.py \
    --s3-bucket scraper-testing \
    --aws-profile localstack \
    --environment dev \
    --log-level INFO "$@"

echo ""
echo "=== S3 Files ==="
aws --profile localstack s3 ls s3://scraper-testing/sourcing/miso_wind_forecast/ --recursive

echo ""
echo "=== Redis Keys ==="
redis-cli KEYS "hash:dev:miso_wind_forecast:*"
