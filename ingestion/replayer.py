"""Event Replayer: pushes historical REES46 events into Kafka in event_time order.

Reads the raw CSV directly (no dependency on Pair 1's Parquet), converts each
row to the Kafka contract (common.schemas.Event) and sends it to topic
"events" with key = user_id.

"""
from __future__ import annotations

import argparse
import csv
import glob
import signal
import sys
import time
from datetime import datetime
from typing import Callable, Iterable, Iterator, Optional

from common.config import settings
from common.schemas import EVENT_TYPES, Event
from ingestion.inspect_raw import open_text


# ---- Reading ---------------------------------------------------------------

def read_events(
    paths: Iterable[str],
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    event_types: Optional[set[str]] = None,
    stats: Optional[dict] = None,
) -> Iterator[Event]:
    """Yield valid events from one or more CSV files, in file order.

    Malformed rows are skipped and counted in stats["skipped"].
    """
    stats = stats if stats is not None else {}
    stats.setdefault("skipped", 0)
    for path in paths:
        with open_text(path) as f:
            for row in csv.DictReader(f):
                try:
                    ev = Event.from_csv_row(row)
                except (KeyError, ValueError, TypeError):
                    stats["skipped"] += 1
                    continue
                if ev.event_type not in EVENT_TYPES:
                    stats["skipped"] += 1
                    continue
                if event_types and ev.event_type not in event_types:
                    continue
                if start_time and ev.event_time < start_time:
                    continue
                if end_time and ev.event_time >= end_time:
                    return
                yield ev


# ---- Pacing ----------------------------------------------------------------

class Pacer:
   

    def __init__(self, speed: float, max_rate: float = 0.0,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.speed = speed
        self.max_rate = max_rate
        self.clock = clock
        self.sleep = sleep
        self._t0_event: Optional[float] = None
        self._t0_wall: Optional[float] = None
        self._last_send: Optional[float] = None

    def wait(self, event_epoch: float) -> None:
        now = self.clock()
        if self._t0_event is None:
            self._t0_event, self._t0_wall = event_epoch, now

        delay = 0.0
        if self.speed > 0:
            target = self._t0_wall + (event_epoch - self._t0_event) / self.speed
            delay = target - now   # negative when behind schedule or events out of order
        if self.max_rate > 0 and self._last_send is not None:
            delay = max(delay, self._last_send + 1.0 / self.max_rate - now)
        if delay > 0:
            self.sleep(delay)
        self._last_send = self.clock()


# ---- Sending ---------------------------------------------------------------

def make_producer(bootstrap: str):
    from confluent_kafka import Producer  # imported lazily so --dry-run needs no Kafka client

    return Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        "batch.num.messages": 10_000,
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 500_000,
    })


def replay(events: Iterable[Event], send: Callable[[Event], None], pacer: Pacer,
           limit: Optional[int] = None, progress_every: int = 10_000,
           log: Callable[[str], None] = lambda m: print(m, file=sys.stderr)) -> int:
    sent = 0
    started = time.monotonic()
    for ev in events:
        pacer.wait(ev.event_time.timestamp())
        send(ev)
        sent += 1
        if progress_every and sent % progress_every == 0:
            rate = sent / max(time.monotonic() - started, 1e-9)
            log(f"sent={sent:,} rate={rate:,.0f}/s event_time={ev.event_time.isoformat()}")
        if limit and sent >= limit:
            break
    return sent


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", required=True, action="append",
                   help="CSV file or glob; repeat for several files (replayed in the given order)")
    p.add_argument("--bootstrap", default=settings.kafka_bootstrap)
    p.add_argument("--topic", default=settings.kafka_topic_events)
    p.add_argument("--speed", type=float, default=60.0, help="replay speed-up; 0 = no pacing")
    p.add_argument("--max-rate", type=float, default=0.0, help="max events/second; 0 = unlimited")
    p.add_argument("--limit", type=int, default=None, help="stop after N events")
    p.add_argument("--start-time", default=None, help="skip events before this time (ISO 8601)")
    p.add_argument("--end-time", default=None, help="stop at this time (ISO 8601, exclusive)")
    p.add_argument("--event-types", default=None, help="comma-separated filter, e.g. view,cart")
    p.add_argument("--dry-run", action="store_true", help="print JSON to stdout instead of sending")
    p.add_argument("--progress-every", type=int, default=10_000)
    args = p.parse_args(argv)

    paths: list[str] = []
    for pattern in args.file:
        matches = sorted(glob.glob(pattern))
        if not matches:
            p.error(f"no file matches {pattern}")
        paths.extend(matches)

    stats: dict = {}
    events = read_events(
        paths,
        start_time=Event.parse_time(args.start_time) if args.start_time else None,
        end_time=Event.parse_time(args.end_time) if args.end_time else None,
        event_types=set(args.event_types.split(",")) if args.event_types else None,
        stats=stats,
    )
    pacer = Pacer(args.speed, args.max_rate)

    stop = {"flag": False}

    def handle_sigint(signum, frame):  # finish cleanly: flush what is buffered
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_sigint)

    def stoppable(it: Iterable[Event]) -> Iterator[Event]:
        for ev in it:
            if stop["flag"]:
                print("interrupted, flushing...", file=sys.stderr)
                return
            yield ev

    if args.dry_run:
        def send(ev: Event) -> None:
            print(ev.to_json())
        producer = None
    else:
        producer = make_producer(args.bootstrap)
        errors = {"n": 0}

        def on_delivery(err, msg):
            if err is not None:
                errors["n"] += 1
                if errors["n"] <= 10:
                    print(f"delivery failed: {err}", file=sys.stderr)

        def send(ev: Event) -> None:
            while True:
                try:
                    producer.produce(args.topic, key=ev.kafka_key(),
                                     value=ev.to_json().encode(), on_delivery=on_delivery)
                    break
                except BufferError:      # local queue full: let it drain
                    producer.poll(0.5)
            producer.poll(0)

    print(f"replaying {paths} -> {'stdout' if args.dry_run else args.bootstrap + '/' + args.topic} "
          f"(speed={args.speed}, max_rate={args.max_rate})", file=sys.stderr)
    t = time.monotonic()
    sent = replay(stoppable(events), send, pacer, args.limit, args.progress_every)
    if producer is not None:
        remaining = producer.flush(30)
        if remaining:
            print(f"WARNING: {remaining} messages not delivered", file=sys.stderr)
        if errors["n"]:
            print(f"WARNING: {errors['n']} delivery errors", file=sys.stderr)
    print(f"done: sent={sent:,} skipped_rows={stats.get('skipped', 0):,} "
          f"in {time.monotonic() - t:.1f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
