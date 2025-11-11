#!/bin/bash
echo "Waiting for Elasticsearch..."
until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 5
done

echo "Creating transliteration index..."
curl -X PUT "http://elasticsearch:9200/club" \
  -H 'Content-Type: application/json' \
  -d @/usr/share/elasticsearch/config/setup/club-index.json

echo "Index created with transliteration analyzer."
