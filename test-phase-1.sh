#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

PASS=0
FAIL=0

echo ""
echo "Phase 1 API Test Suite"
echo ""

test_endpoint() {
    local name=$1
    local method=$2
    local url=$3
    local data=$4
    local expected_field=$5
    
    echo "Testing: $name ..."
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s "$url")
        http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
    else
        http_code=$(curl -s -o /tmp/response.txt -w "%{http_code}" -X "$method" -H "Content-Type: application/json" -d "$data" "$url")
        response=$(cat /tmp/response.txt)
    fi
    
    if [ "$http_code" = "200" ]; then
        if echo "$response" | grep -q "$expected_field"; then
            echo "$GREEN✓ PASS$NC"
            echo "  Response: $response"
            PASS=$((PASS + 1))
        else
            echo "$RED✗ FAIL$NC (missing: $expected_field)"
            echo "  Response: $response"
            FAIL=$((FAIL + 1))
        fi
    else
        echo "$RED✗ FAIL$NC (HTTP $http_code)"
        echo "  Response: $response"
        FAIL=$((FAIL + 1))
    fi
    echo ""
}

echo "1. Testing Health Checks"
test_endpoint "Broker-1 /health" "GET" "http://localhost:5001/health" "" "status"
test_endpoint "Broker-2 /health" "GET" "http://localhost:5002/health" "" "status"
test_endpoint "Broker-3 /health" "GET" "http://localhost:5003/health" "" "status"

echo ""
echo "2. Testing Health Status"
test_endpoint "Broker-1 /api/health-status" "GET" "http://localhost:5001/api/health-status" "" "healthy"
test_endpoint "Broker-2 /api/health-status" "GET" "http://localhost:5002/api/health-status" "" "healthy"
test_endpoint "Broker-3 /api/health-status" "GET" "http://localhost:5003/api/health-status" "" "healthy"

echo ""
echo "3. Testing Replication Status"
test_endpoint "Broker-1 /api/replication-status" "GET" "http://localhost:5001/api/replication-status" "" "queue_size"
test_endpoint "Broker-2 /api/replication-status" "GET" "http://localhost:5002/api/replication-status" "" "queue_size"
test_endpoint "Broker-3 /api/replication-status" "GET" "http://localhost:5003/api/replication-status" "" "queue_size"

echo ""
echo "4. Testing Heartbeat POST"
heartbeat_payload='{"broker_id":"test","lamport_clock":1,"timestamp":1234567890,"status":"healthy"}'
test_endpoint "Broker-1 POST /api/heartbeat" "POST" "http://localhost:5001/api/heartbeat" "$heartbeat_payload" "status"
test_endpoint "Broker-2 POST /api/heartbeat" "POST" "http://localhost:5002/api/heartbeat" "$heartbeat_payload" "status"
test_endpoint "Broker-3 POST /api/heartbeat" "POST" "http://localhost:5003/api/heartbeat" "$heartbeat_payload" "status"

echo ""
echo "5. Testing Sync Request"
sync_payload='{"broker_id":"test","last_known_seq":0}'
test_endpoint "Broker-1 POST /api/sync-request" "POST" "http://localhost:5001/api/sync-request" "$sync_payload" "sync_response"

echo ""
echo ""
echo "Results: Passed=$PASS Failed=$FAIL"
echo ""

if [ $FAIL -eq 0 ]; then
    echo "All Phase 1 tests passed!"
    exit 0
else
    echo "Some tests failed"
    exit 1
fi
