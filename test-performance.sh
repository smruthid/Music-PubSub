#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# test-performance.sh — Performance evaluation test harness
#
# Measures latency, reliability, and throughput under load.
#
# Usage:
#   1. In terminal 1:  docker compose down -v && docker compose up --build --scale broker=2
#   2. In terminal 2:  bash test-performance.sh
# ─────────────────────────────────────────────────────────────────────

set -euo pipefail

SEED="https://localhost:5001"
BSECRET="super_secret_broker_key_2026"
CURL="curl -sk"             # silent + allow self-signed certs
NUM_EVENTS=20               # number of events to publish for load test

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       PERFORMANCE EVALUATION TEST HARNESS                ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─── Wait for cluster ────────────────────────────────────────────
echo "⏳ Waiting for seed-broker to be ready..."
for i in $(seq 1 30); do
  if $CURL "$SEED/health" 2>/dev/null | grep -q '"status":"ok"'; then
    echo "   ✅ Seed broker is up."
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then echo "   ❌ Seed broker not ready after 30s. Aborting."; exit 1; fi
done

sleep 5  # let non-seed brokers join
echo ""

# ─── Cluster info ────────────────────────────────────────────────
echo "📡 Cluster members:"
$CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/cluster/members" | python3 -m json.tool
echo ""

# ─── Register + Login ────────────────────────────────────────────
echo "👤 Registering test user..."
$CURL -X POST "$SEED/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"perftest","email":"perf@test.com","password":"pass123"}' > /dev/null 2>&1 || true

TOKEN=$($CURL -X POST "$SEED/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"perftest","password":"pass123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "   Token: ${TOKEN:0:20}..."
echo ""

# ─── Subscribe ───────────────────────────────────────────────────
echo "🎵 Creating subscription (Rock, San Francisco, CA)..."
$CURL -X POST "$SEED/subscriptions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"genre":"Rock","city":"San Francisco","state":"CA","start_date":"2026-01-01","end_date":"2026-12-31"}' > /dev/null 2>&1 || true
echo "   ✅ Subscribed."
echo ""

# ─── Reset metrics ───────────────────────────────────────────────
echo "🔄 Resetting metrics on seed broker..."
$CURL -X POST -H "X-Broker-Secret: $BSECRET" "$SEED/api/metrics/reset" | python3 -m json.tool
echo ""

# ─── Publish events under load ───────────────────────────────────
echo "🚀 Publishing $NUM_EVENTS events to seed-broker..."
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
      \"event_date_time\":\"2026-07-0${i}T20:00:00Z\",
      \"priority\":\"normal\"
    }" > /dev/null 2>&1 &
done

# Wait for all background curl processes to finish
wait

END_TIME=$(python3 -c "import time; print(int(time.time()*1000))")
PUBLISH_DURATION=$((END_TIME - START_TIME))
echo "   ✅ All $NUM_EVENTS events published in ${PUBLISH_DURATION}ms"
echo ""

# ─── Wait for gossip propagation ─────────────────────────────────
echo "⏳ Waiting 15 seconds for gossip propagation + queue drain..."
sleep 15
echo ""

# ─── Fetch metrics from seed broker ─────────────────────────────
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              SEED BROKER METRICS                         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

METRICS=$($CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/metrics")

echo "$METRICS" | python3 -c "
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
    print(f'  Delivery ratio:      N/A (no local publishes to compare)')
print(f'  Duplicates received: {rel[\"duplicates_received\"]}')
print(f'  Gossip rounds sent:  {rel[\"gossip_rounds_sent\"]}')
print(f'  Gossip msgs sent:    {rel[\"gossip_messages_sent\"]}')
print()
print('  ── Latency (ms) ────────────────────────────────')
print(f'  Samples:             {lat[\"count\"]}')
print(f'  Min:                 {lat[\"min\"]}')
print(f'  Max:                 {lat[\"max\"]}')
print(f'  Avg:                 {lat[\"avg\"]}')
print(f'  p50:                 {lat[\"p50\"]}')
print(f'  p95:                 {lat[\"p95\"]}')
print(f'  p99:                 {lat[\"p99\"]}')
print()
print('  ── Throughput ({thr[\"window_seconds\"]}s window) ─────────────────────')
print(f'  Publishes in window: {thr[\"publishes_in_window\"]}')
print(f'  Deliveries in window:{thr[\"deliveries_in_window\"]}')
print(f'  Publish rate:        {thr[\"publish_rate_per_sec\"]}/sec')
print(f'  Delivery rate:       {thr[\"delivery_rate_per_sec\"]}/sec')
print()
print('  ── Per-Broker Deliveries ────────────────────────')
for origin, count in m.get('per_broker_deliveries', {}).items():
    print(f'    {origin}: {count}')
"

echo ""

# ─── Fetch replication status ────────────────────────────────────
echo "── Replication Status ──"
$CURL -H "X-Broker-Secret: $BSECRET" "$SEED/api/replication-status" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  Queue size:    {d[\"queue_size\"]}')
print(f'  Seen messages: {d[\"seen_messages\"]}')
print(f'  Max hops:      {d[\"max_hops\"]}')
print(f'  Lamport clock: {d[\"lamport_clock\"]}')
"

echo ""

# ─── Verify all events in DB ─────────────────────────────────────
echo "── Event Delivery Verification ──"
$CURL -H "Authorization: Bearer $TOKEN" "$SEED/events" | python3 -c "
import sys, json
data = json.load(sys.stdin)
count = len(data.get('events', []))
print(f'  Events in database: {count}')
if count >= $NUM_EVENTS:
    print(f'  ✅ PASS: All $NUM_EVENTS events found in DB')
else:
    print(f'  ⚠️  Only {count}/{$NUM_EVENTS} events in DB')
"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           PERFORMANCE TEST COMPLETE                      ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "KEY THINGS TO VERIFY:"
echo "  1. Delivery ratio should be > 0 on non-seed brokers"
echo "  2. Latency p50 should be < 5000ms for a local Docker cluster"
echo "  3. All $NUM_EVENTS events should appear in the database"
echo "  4. Duplicates received > 0 proves dedup is working"
echo "  5. Queue drains to 0 (gossip stops chattering)"
echo ""