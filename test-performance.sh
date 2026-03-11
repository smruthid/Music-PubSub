#   1. In terminal 1:  docker compose down -v && docker compose up --build --scale broker=2
#   2. In terminal 2:  bash test-performance.sh

set -euo pipefail

SEED="https://localhost:5001"
BSECRET="super_secret_broker_key_2026"
CURL="curl -sk"            
NUM_EVENTS=20               

docker_fetch() {
  local ADDR="$1"
  local URL_PATH="$2"
  local METHOD="${3:-GET}"
  local PEER_HOST="${ADDR%%:*}"
  local PEER_PORT="${ADDR##*:}"

  docker compose exec -T seed-broker node -e "
    const https = require('https');
    const options = {
      hostname: '$PEER_HOST',
      port: $PEER_PORT,
      path: '$URL_PATH',
      method: '$METHOD',
      rejectUnauthorized: false,
      headers: { 'X-Broker-Secret': '$BSECRET' }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => process.stdout.write(data));
    });
    req.on('error', (e) => { process.stderr.write(e.message); process.exit(1); });
    req.end();
  " 2>/dev/null
}

echo ""
echo "       PERFORMANCE EVALUATION TEST HARNESS                "
echo ""

echo "⏳ Waiting for seed-broker to be ready..."
for i in $(seq 1 30); do
  if $CURL "$SEED/health" 2>/dev/null | grep -q '"status":"ok"'; then
    echo "Seed broker is up."
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then echo "   ❌ Seed broker not ready after 30s. Aborting."; exit 1; fi
done

sleep 5  
echo ""

echo "Cluster members:"
$CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members" | python3 -m json.tool
echo ""

echo "Discovering non-seed broker addresses..."
PEER_HOSTS=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data.get('peers', []):
    print(f'{p[\"host\"]}:{p[\"port\"]}')
")

PEER_ARRAY=()
while IFS= read -r line; do
  [ -n "$line" ] && PEER_ARRAY+=("$line")
done <<< "$PEER_HOSTS"

echo "   Found ${#PEER_ARRAY[@]} peer broker(s) (Docker-internal addresses)"
for p in "${PEER_ARRAY[@]}"; do
  echo "     - $p"
done
echo ""

echo "Registering test user..."
$CURL -X POST "$SEED/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"perftest","email":"perf@test.com","password":"pass123"}' > /dev/null 2>&1 || true

TOKEN=$($CURL -X POST "$SEED/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"perftest","password":"pass123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "   Token: ${TOKEN:0:20}..."
echo ""

echo "🎵 Creating subscription (Rock, San Francisco, CA)..."
$CURL -X POST "$SEED/subscriptions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"genre":"Rock","city":"San Francisco","state":"CA","start_date":"2026-01-01","end_date":"2026-12-31"}' > /dev/null 2>&1 || true
echo "Subscribed."
echo ""

echo "Resetting metrics on all brokers..."
$CURL -X POST -H "X-Broker-Secret: $BSECRET" "$SEED/api/metrics/reset" > /dev/null 2>&1
for PEER in "${PEER_ARRAY[@]}"; do
  docker_fetch "$PEER" "/api/metrics/reset" "POST" > /dev/null 2>&1 || true
done
echo "All metrics reset."
echo ""

echo "Publishing $NUM_EVENTS events to seed-broker..."
START_TIME=$(python3 -c "import time; print(int(time.time()*1000))")

for i in $(seq 1 $NUM_EVENTS); do
  $CURL -X POST "$SEED/events" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{
      \"title\":\"Load Test Concert $i\",
      \"artist\":\"Artist $i\",
      \"genre\":\"Rock\",
      \"city\":\"San Francisco\",
      \"state\":\"CA\",
      \"venue\":\"Venue $i\",
      \"event_date_time\":\"2026-07-01T20:00:00Z\",
      \"priority\":\"normal\"
    }" > /dev/null 2>&1 &
done

wait

END_TIME=$(python3 -c "import time; print(int(time.time()*1000))")
PUBLISH_DURATION=$((END_TIME - START_TIME))
echo "All $NUM_EVENTS events published in ${PUBLISH_DURATION}ms"
echo ""

echo "Waiting 15 seconds for gossip propagation + queue drain..."
sleep 15
echo ""

echo "  SEED BROKER METRICS (publisher)"
echo ""

$CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/metrics" | python3 -c "
import sys, json

m = json.load(sys.stdin)
rel = m['reliability']
lat = m['latency']
thr = m['throughput']

print(f'  Broker:              {m[\"broker_id\"]}')
print(f'  Uptime:              {m[\"uptime_human\"]}')
print()
print('  ── Reliability ──────────────────────────────────')
print(f'  Events published:    {rel[\"events_published\"]}')
print(f'  Events delivered:    {rel[\"events_delivered\"]}')
ratio = rel['delivery_ratio']
if ratio is not None:
    print(f'  Delivery ratio:      {ratio * 100:.1f}%')
else:
    print(f'  Delivery ratio:      N/A')
print(f'  Duplicates received: {rel[\"duplicates_received\"]}')
print(f'  Gossip rounds sent:  {rel[\"gossip_rounds_sent\"]}')
print(f'  Gossip msgs sent:    {rel[\"gossip_messages_sent\"]}')
print()
print('  ── Latency (ms) ────────────────────────────────')
print(f'  Samples:             {lat[\"count\"]}')
if lat['count'] > 0:
    print(f'  Min:                 {lat[\"min\"]}')
    print(f'  Max:                 {lat[\"max\"]}')
    print(f'  Avg:                 {lat[\"avg\"]}')
    print(f'  p50:                 {lat[\"p50\"]}')
    print(f'  p95:                 {lat[\"p95\"]}')
    print(f'  p99:                 {lat[\"p99\"]}')
else:
    print('  (no samples — this broker was the publisher, not a receiver)')
print()
print(f'  ── Throughput ({thr[\"window_seconds\"]}s window) ─────────────────────')
print(f'  Publishes in window: {thr[\"publishes_in_window\"]}')
print(f'  Deliveries in window:{thr[\"deliveries_in_window\"]}')
print(f'  Publish rate:        {thr[\"publish_rate_per_sec\"]}/sec')
print(f'  Delivery rate:       {thr[\"delivery_rate_per_sec\"]}/sec')
print()
print('  ── Per-Broker Deliveries ────────────────────────')
pbd = m.get('per_broker_deliveries', {})
if pbd:
    for origin, count in pbd.items():
        print(f'    {origin}: {count}')
else:
    print('    (none — this broker published locally, no gossip deliveries)')
"
echo ""

PEER_NUM=1
for PEER in "${PEER_ARRAY[@]}"; do
  echo ""
  echo "  PEER BROKER #$PEER_NUM METRICS (gossip receiver)"
  echo ""

  RAW=$(docker_fetch "$PEER" "/api/metrics" 2>/dev/null) || true

  if [ -z "$RAW" ]; then
    echo "Could not reach peer broker at $PEER"
  else
    echo "$RAW" | python3 -c "
import sys, json

m = json.load(sys.stdin)
rel = m['reliability']
lat = m['latency']
thr = m['throughput']

print(f'  Broker:              {m[\"broker_id\"]}')
print(f'  Uptime:              {m[\"uptime_human\"]}')
print()
print('  ── Reliability ──────────────────────────────────')
print(f'  Events published:    {rel[\"events_published\"]}')
print(f'  Events delivered:    {rel[\"events_delivered\"]}')
ratio = rel['delivery_ratio']
if ratio is not None:
    print(f'  Delivery ratio:      {ratio * 100:.1f}%')
else:
    print(f'  Delivery ratio:      N/A')
print(f'  Duplicates received: {rel[\"duplicates_received\"]}')
print(f'  Gossip rounds sent:  {rel[\"gossip_rounds_sent\"]}')
print(f'  Gossip msgs sent:    {rel[\"gossip_messages_sent\"]}')
print()
print('  ── Latency (ms) ────────────────────────────────')
print(f'  Samples:             {lat[\"count\"]}')
if lat['count'] > 0:
    print(f'  Min:                 {lat[\"min\"]}')
    print(f'  Max:                 {lat[\"max\"]}')
    print(f'  Avg:                 {lat[\"avg\"]}')
    print(f'  p50:                 {lat[\"p50\"]}')
    print(f'  p95:                 {lat[\"p95\"]}')
    print(f'  p99:                 {lat[\"p99\"]}')
else:
    print('  (no latency samples)')
print()
print(f'  ── Throughput ({thr[\"window_seconds\"]}s window) ─────────────────────')
print(f'  Publishes in window: {thr[\"publishes_in_window\"]}')
print(f'  Deliveries in window:{thr[\"deliveries_in_window\"]}')
print(f'  Publish rate:        {thr[\"publish_rate_per_sec\"]}/sec')
print(f'  Delivery rate:       {thr[\"delivery_rate_per_sec\"]}/sec')
print()
print('  ── Per-Broker Deliveries ────────────────────────')
pbd = m.get('per_broker_deliveries', {})
if pbd:
    for origin, count in pbd.items():
        print(f'    {origin}: {count}')
else:
    print('    (none)')
"
  fi
  echo ""
  PEER_NUM=$((PEER_NUM + 1))
done

echo "── Replication Status (seed) ──"
$CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/replication-status" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  Queue size:    {d[\"queue_size\"]}')
print(f'  Seen messages: {d[\"seen_messages\"]}')
print(f'  Max hops:      {d[\"max_hops\"]}')
print(f'  Lamport clock: {d[\"lamport_clock\"]}')
"
echo ""

echo "── Event Delivery Verification ──"
$CURL -H "Authorization: Bearer $TOKEN" "$SEED/events" | python3 -c "
import sys, json
data = json.load(sys.stdin)
count = len(data.get('events', []))
print(f'  Events in database: {count}')
if count >= $NUM_EVENTS:
    print(f'PASS: All $NUM_EVENTS events found in DB')
else:
    print(f'  ⚠️  Only {count}/$NUM_EVENTS events in DB')
"
echo ""
echo "           CROSS-CLUSTER SUMMARY"
echo ""

SEED_METRICS=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/metrics" 2>/dev/null)

ALL_PEER_JSON="["
FIRST=true
for PEER in "${PEER_ARRAY[@]}"; do
  PM=$(docker_fetch "$PEER" "/api/metrics" 2>/dev/null) || true
  if [ -n "$PM" ]; then
    if [ "$FIRST" = true ]; then FIRST=false; else ALL_PEER_JSON+=","; fi
    ALL_PEER_JSON+="$PM"
  fi
done
ALL_PEER_JSON+="]"

python3 << PYEOF
import json

seed = json.loads(r'''$SEED_METRICS''')
peer_metrics = json.loads(r'''$ALL_PEER_JSON''')

total_published = seed['reliability']['events_published']
total_delivered = sum(m['reliability']['events_delivered'] for m in peer_metrics)
total_duplicates = seed['reliability']['duplicates_received'] + sum(m['reliability']['duplicates_received'] for m in peer_metrics)

all_latencies = [m['latency'] for m in peer_metrics if m['latency']['count'] > 0]

print(f'  Total events published (seed):          {total_published}')
print(f'  Total events delivered (across peers):   {total_delivered}')
print(f'  Number of peer brokers reporting:         {len(peer_metrics)}')
if len(peer_metrics) > 0:
    expected = total_published * len(peer_metrics)
    ratio = total_delivered / expected * 100 if expected > 0 else 0
    print(f'  Expected deliveries ({total_published} {len(peer_metrics)}):       {expected}')
    print(f'  Actual deliveries:                       {total_delivered}')
    print(f'  Cross-cluster reliability:               {ratio:.1f}%')
print(f'  Total duplicates (all brokers):          {total_duplicates}')
print()

if all_latencies:
    avg_of_avgs = sum(l['avg'] for l in all_latencies) / len(all_latencies)
    max_p99 = max(l['p99'] for l in all_latencies)
    min_min = min(l['min'] for l in all_latencies)
    print(f'  Gossip latency (across receiving peers):')
    print(f'    Min:     {min_min} ms')
    print(f'    Avg:     {avg_of_avgs:.1f} ms')
    print(f'    p99 max: {max_p99} ms')
    if avg_of_avgs < 5000:
        print(f' PASS: Average latency under 5000ms')
    else:
        print(f'  Average latency exceeds 5000ms target')
else:
    print('  No latency data from peers')
PYEOF

echo ""
echo "           PERFORMANCE TEST COMPLETE"
echo ""
