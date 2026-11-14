"""Send Wikipedia change events to Kafka.

Live mode reads the Wikimedia EventStreams feed (server-sent events) and
forwards every event, unchanged, to a Kafka topic. Filtering is left to Spark.

    python -m src.streaming.producer --minutes 30

Replay mode sends events from a JSON-lines file instead. It is used to test
the Spark job and its watermark with known, partly late, events. Two extra
fields drive the replay and are removed before sending:
    _lag_s            the event timestamp is set to "now - _lag_s"
    _pause_before_s   wait this long before sending the event

    python -m src.streaming.producer --replay src/streaming/sample_events.jsonl --topic wiki-test

Requires kafka-python (pip install "kafka-python>=2.0.3").
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "ECE-student-project/1.0 (IMDb big data course; contact via GitHub JuliaEzzedine)"


def make_producer() -> KafkaProducer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
    print(f"[kafka] connecting to {bootstrap}")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=200,
    )


def sse_events(last_event_id=None):
    """Yield (event_id, payload) from the Wikimedia SSE feed."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    req = urllib.request.Request(STREAM_URL, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as resp:
        event_id, data = None, []
        for raw in resp:
            line = raw.decode("utf-8").rstrip("\r\n")
            if line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
            elif line == "" and data:
                yield event_id, "\n".join(data)
                data = []


def run_live(producer, topic, minutes):
    deadline = time.time() + minutes * 60
    sent, last_id, last_log = 0, None, time.time()
    while time.time() < deadline:
        try:
            for event_id, payload in sse_events(last_id):
                last_id = event_id
                try:
                    event = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if event.get("meta", {}).get("domain") == "canary":
                    continue  # Wikimedia monitoring events
                producer.send(topic, key=event.get("title"), value=event)
                sent += 1
                if time.time() - last_log > 30:
                    print(f"[live] {sent} events sent to '{topic}'", flush=True)
                    last_log = time.time()
                if time.time() >= deadline:
                    break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            # The feed closes connections from time to time: reconnect politely,
            # resuming after the last event received.
            print(f"[live] connection lost ({exc}), reconnecting in 5 s")
            time.sleep(5)
    producer.flush()
    print(f"[done] {sent} events sent in {minutes} min")


def run_replay(producer, topic, path):
    sent = 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            event = json.loads(line)
            pause = event.pop("_pause_before_s", 0)
            lag = event.pop("_lag_s", 0)
            if pause:
                producer.flush()
                print(f"[replay] waiting {pause} s ...", flush=True)
                time.sleep(pause)
            ts = int(time.time() - lag)
            event["timestamp"] = ts
            meta = event.setdefault("meta", {})
            meta.setdefault("id", str(uuid.uuid4()))
            meta["dt"] = datetime.fromtimestamp(ts, timezone.utc).isoformat()
            producer.send(topic, key=event.get("title"), value=event)
            sent += 1
            print(f"[replay] {event.get('wiki')} | {event.get('title')} | {lag} s late")
    producer.flush()
    print(f"[done] {sent} events replayed to '{topic}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="wiki-changes")
    parser.add_argument("--minutes", type=float, default=30, help="live mode duration")
    parser.add_argument("--replay", help="JSON-lines file to replay instead of the live feed")
    args = parser.parse_args()

    producer = make_producer()
    if args.replay:
        run_replay(producer, args.topic, args.replay)
    else:
        run_live(producer, args.topic, args.minutes)
    producer.close()


if __name__ == "__main__":
    main()
