#   1. docker compose down -v && docker compose up --build --scale broker=2
#   2. bash test-network-partition.sh

set -euo pipefail

SEED="https://localhost:5001"
BSECRET="super_secret_broker_key_2026"
CURL="curl -sk"
NETWORK=$(docker network ls --format '{{.Name}}' | grep pubsub-network | head -1)

echo ""
echo " Network Partition Test"
echo "Network: $NETWORK"
echo ""

echo "Waiting for cluster..."
for i in $(seq 1 30); do
  $CURL "$SEED/health" 2>/dev/null | grep -q '"status":"ok"' && break
  sleep 1
done
sleep 8

CLUSTER=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members")
echo "Cluster: $(echo "$CLUSTER" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'size={d[\"cluster_size\"]}')")"

CONTAINER=$(docker compose ps | grep -E 'broker-[0-9]+' | awk '{print $1}' | head -1)
echo "Target container: $CONTAINER"

$CURL -X POST "$SEED/auth/register" -H "Content-Type: application/json" \
  -d '{"username":"nettest","email":"net@test.com","password":"pass123"}' > /dev/null 2>&1 || true
TOKEN=$($CURL -X POST "$SEED/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"nettest","password":"pass123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")
$CURL -X POST "$SEED/subscriptions" -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"genre":"Rock","city":"San Francisco","state":"CA","start_date":"2026-01-01","end_date":"2026-12-31"}' > /dev/null 2>&1 || true

echo ""
echo "── 1. Disconnect broker from network ──"
docker network disconnect "$NETWORK" "$CONTAINER"
echo "Disconnected $CONTAINER. Waiting 20s for heartbeat detection..."
sleep 20

HEALTH=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/health-status")
echo "Cluster health after partition:"
echo "$HEALTH" | python3 -c "
import sys,json
d = json.load(sys.stdin)
for bid, info in d.get('cluster_health',{}).items():
    status = 'healthy' if info.get('healthy') else 'UNHEALTHY'
    print(f'  {bid}: {status} (missed={info.get(\"missedHeartbeats\",0)})')
"

echo ""
echo "── 2. Publish events during partition ──"
for i in $(seq 1 3); do
  $CURL -X POST "$SEED/events" -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
    -d "{\"title\":\"Partition Event $i\",\"artist\":\"Artist $i\",\"genre\":\"Rock\",\"city\":\"San Francisco\",\"state\":\"CA\",\"venue\":\"Venue\",\"event_date_time\":\"2026-08-0${i}T20:00:00Z\",\"priority\":\"normal\"}" > /dev/null 2>&1
done
echo "Published 3 events while broker is partitioned."

echo ""
echo "── 3. Reconnect broker ──"
docker network connect "$NETWORK" "$CONTAINER"
echo "Reconnected. Waiting 20s for rejoin + gossip..."
sleep 20

CLUSTER2=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members")
echo "Cluster after recovery: $(echo "$CLUSTER2" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'size={d[\"cluster_size\"]}')")"

echo ""
echo "── 4. Publish post-recovery event ──"
$CURL -X POST "$SEED/events" -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"title":"Post Recovery Event","artist":"Recovery Artist","genre":"Rock","city":"San Francisco","state":"CA","venue":"Venue","event_date_time":"2026-09-01T20:00:00Z","priority":"normal"}' > /dev/null 2>&1
echo "Published. Waiting 10s for gossip..."
sleep 10

echo ""
echo "── 5. Verify events in database ──"
EVENT_COUNT=$($CURL -H "Authorization: Bearer $TOKEN" "$SEED/events" | python3 -c "
import sys,json
events = json.load(sys.stdin).get('events',[])
test_events = [e for e in events if 'Partition' in e['title'] or 'Recovery' in e['title']]
print(len(test_events))
")
echo "Test events in DB: $EVENT_COUNT/4"

echo ""
echo "── 6. Crash test (docker stop/start) ──"
docker stop "$CONTAINER" > /dev/null 2>&1
echo "Stopped $CONTAINER. Waiting 20s..."
sleep 20

HEALTH2=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/health-status")
echo "Health after crash:"
echo "$HEALTH2" | python3 -c "
import sys,json
d = json.load(sys.stdin)
for bid, info in d.get('cluster_health',{}).items():
    status = 'healthy' if info.get('healthy') else 'UNHEALTHY'
    print(f'  {bid}: {status}')
"

docker start "$CONTAINER" > /dev/null 2>&1
echo "Restarted $CONTAINER. Waiting 20s for rejoin..."
sleep 20

CLUSTER3=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members")
echo "Cluster after crash recovery: $(echo "$CLUSTER3" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'size={d[\"cluster_size\"]}')")"

echo ""
echo "── 7. Seed broker crash (docker stop/start) ──"
SEED_CONTAINER=$(docker compose ps | grep seed-broker | awk '{print $1}' | head -1)
echo "Stopping seed broker ($SEED_CONTAINER)..."
docker stop "$SEED_CONTAINER" > /dev/null 2>&1
echo "Seed broker is down. Waiting 10s..."
sleep 10

ALIVE=$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || echo "false")
echo "Non-seed broker still running: $ALIVE"

echo "Restarting seed broker..."
docker start "$SEED_CONTAINER" > /dev/null 2>&1
echo "Waiting 25s for seed to recover and cluster to reform..."
sleep 25

SEED_UP="false"
for i in $(seq 1 10); do
  if $CURL "$SEED/health" 2>/dev/null | grep -q '"status":"ok"'; then
    SEED_UP="true"
    break
  fi
  sleep 2
done
echo "Seed broker responsive: $SEED_UP"

CLUSTER4=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members")
echo "Cluster after seed recovery: $(echo "$CLUSTER4" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'size={d[\"cluster_size\"]}')")"

echo ""
echo "Done"
