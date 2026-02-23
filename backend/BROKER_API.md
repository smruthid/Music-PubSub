# Inter-Broker Communication API Specification

## Overview
This document defines the message format and communication protocol for inter-broker gossip and replication.

## Message Types

### 1. Heartbeat Message
Used to track broker liveness in the cluster.

```json
{
  "type": "heartbeat",
  "broker_id": "broker-1",
  "timestamp": 1677000000000,
  "lamport_clock": 42,
  "status": "healthy",
  "message_queue_length": 150
}
```

### 2. Gossip Message (Event Propagation)
Used to propagate published events across brokers.

```json
{
  "type": "gossip",
  "broker_id": "broker-1",
  "lamport_clock": 45,
  "timestamp": 1677000001000,
  "events": [
    {
      "event_id": "evt_123",
      "title": "Concert XYZ",
      "artist": "Artist Name",
      "genre": "Rock",
      "city": "San Francisco",
      "state": "CA",
      "venue": "Venue Name",
      "event_date_time": "2024-03-15T19:00:00Z",
      "priority": "normal",
      "published_at": "2024-02-23T10:00:00Z",
      "replicated": false
    }
  ],
  "seq_number": 1001
}
```

### 3. Replication Acknowledgment
Confirms receipt of gossip messages.

```json
{
  "type": "replication_ack",
  "broker_id": "broker-1",
  "lamport_clock": 46,
  "timestamp": 1677000002000,
  "ack_seq": 1001,
  "status": "success"
}
```

### 4. Broker Sync Request
Request full state sync from a peer broker.

```json
{
  "type": "sync_request",
  "broker_id": "broker-2",
  "lamport_clock": 50,
  "timestamp": 1677000003000,
  "last_known_seq": 995
}
```

### 5. Broker Sync Response
Responds with broker's current state.

```json
{
  "type": "sync_response",
  "broker_id": "broker-1",
  "lamport_clock": 51,
  "timestamp": 1677000004000,
  "current_seq": 1001,
  "events": [
    // Array of all events with seq > 995
  ]
}
```

## Communication Protocol

- **Port**: Each broker listens on its designated port (5001, 5002, 5003)
- **Protocol**: HTTP/JSON with WebSocket upgrade for persistent connections
- **Timeout**: 5 seconds for inter-broker communication
- **Retry Policy**: Exponential backoff (1s, 2s, 4s, 8s max)

## Lamport Clock
Every message includes a Lamport clock value for causal ordering:
- Increment own clock before sending
- Set clock = max(own_clock, received_clock) + 1 on receipt
- Use clock for ordering events across distributed system