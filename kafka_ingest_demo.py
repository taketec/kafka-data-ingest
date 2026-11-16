#!/usr/bin/env python3
"""
Kafka high-velocity ingestion demo with a live terminal dashboard.

What it does
  1. Connects to a local Kafka broker and creates a fresh topic.
  2. Starts consumer processes (one consumer group) that parse every event
     and aggregate counts + revenue. This is the "ingestion" side.
  3. Fires events at the broker as fast as it can for --duration seconds
     (batched + lz4-compressed) while redrawing a live dashboard.
  4. Waits for the consumers to drain, prints a summary, deletes the topic.

Setup:  docker compose up -d
        pip install confluent-kafka
Run:    python kafka_ingest_demo.py

Note: events are formatted from small pre-generated pools so Python's own
overhead stays low. Every event still has a unique id and timestamp, and every
event is fully parsed with json.loads on the consumer side.
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import signal
import sys
import time
from collections import deque

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

TYPES = ["page_view", "search", "add_to_cart", "login", "purchase"]
WEIGHTS = [50, 20, 15, 10, 5]
DEVICES = ["ios", "android", "web", "tablet"]
TYPE_INDEX = {t: i for i, t in enumerate(TYPES)}
TEMPLATE = (b'{"id":%d,"ts":%d,"type":"%s","user":%d,'
            b'"device":"%s","amount":%d,"pad":"%s"}')
POOL = 4096  # must be a power of two
MASK = POOL - 1
SPARK = "▁▂▃▄▅▆▇█"
SPARK_W = 24

BOLD, DIM, RESET = "\x1b[1m", "\x1b[2m", "\x1b[0m"
CYAN, GREEN, YELLOW, RED = "\x1b[36m", "\x1b[32m", "\x1b[33m", "\x1b[31m"


# --------------------------------------------------------------------------
# Consumer side (runs in separate processes so it never fights the producer
# for Python's GIL)
# --------------------------------------------------------------------------
class Shared:
    """Counters shared between the consumer processes and the dashboard."""

    def __init__(self, ctx):
        self.consumed = ctx.Value("q", 0)
        self.nbytes = ctx.Value("q", 0)
        self.cents = ctx.Value("q", 0)
        self.errors = ctx.Value("q", 0)
        self.ready = ctx.Value("i", 0)
        self.by_type = ctx.Array("q", len(TYPES))
        self.stop = ctx.Event()


def consumer_worker(bootstrap, topic, group, shared):
    # The parent handles Ctrl+C and tells us to stop via shared.stop.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    joined = [False]

    def on_assign(consumer, partitions):
        if not joined[0]:
            joined[0] = True
            with shared.ready.get_lock():
                shared.ready.value += 1

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 65536,
        "fetch.wait.max.ms": 50,
    })
    consumer.subscribe([topic], on_assign=on_assign)
    loads = json.loads
    try:
        while not shared.stop.is_set():
            msgs = consumer.consume(num_messages=5000, timeout=0.2)
            if not msgs:
                continue
            n = size = cents = errs = 0
            tally = [0] * len(TYPES)
            for m in msgs:
                if m.error():
                    errs += 1
                    continue
                raw = m.value()
                ev = loads(raw)
                tally[TYPE_INDEX[ev["type"]]] += 1
                cents += ev["amount"]
                size += len(raw)
                n += 1
            with shared.consumed.get_lock():
                shared.consumed.value += n
            with shared.nbytes.get_lock():
                shared.nbytes.value += size
            with shared.cents.get_lock():
                shared.cents.value += cents
            if errs:
                with shared.errors.get_lock():
                    shared.errors.value += errs
            with shared.by_type.get_lock():
                for i, c in enumerate(tally):
                    shared.by_type[i] += c
    finally:
        consumer.close()


# --------------------------------------------------------------------------
# Producer side
# --------------------------------------------------------------------------
class ProducerStats:
    __slots__ = ("delivered", "failed")

    def __init__(self):
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, err, msg):
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1


def build_pools():
    rng = random.Random(7)
    kinds = rng.choices(range(len(TYPES)), WEIGHTS, k=POOL)
    users = [rng.randrange(1, 100_000) for _ in range(POOL)]
    p_type = [TYPES[k].encode() for k in kinds]
    p_key = [str(u).encode() for u in users]  # key = user id, so Kafka spreads load across partitions
    p_dev = [rng.choice(DEVICES).encode() for _ in range(POOL)]
    p_amt = [rng.randrange(500, 50_000) if TYPES[k] == "purchase" else 0 for k in kinds]
    return p_type, users, p_key, p_dev, p_amt


def make_pad(size):
    sample = TEMPLATE % (1_000_000, 1_700_000_000_000, b"add_to_cart", 99_999,
                         b"android", 49_999, b"")
    return b"x" * max(0, size - len(sample))


# --------------------------------------------------------------------------
# Terminal dashboard
# --------------------------------------------------------------------------
def spark(values):
    vals = list(values)
    if not vals:
        return " " * SPARK_W
    hi = max(vals) or 1
    s = "".join(SPARK[min(7, int(v / hi * 7.999))] for v in vals)
    return s.ljust(SPARK_W)


class Dashboard:
    def __init__(self, args, topic, n_consumers):
        self.args = args
        self.topic = topic
        self.n_consumers = n_consumers
        self.tty = sys.stdout.isatty()
        self.nlines = 0
        self.t0 = time.monotonic()
        self.prev = None
        self.last_plain = 0.0
        self.prod_hist = deque(maxlen=SPARK_W)
        self.cons_hist = deque(maxlen=SPARK_W)
        self.prod_rate = self.cons_rate = 0.0
        self.prod_mbps = self.cons_mbps = 0.0
        self.prod_peak = self.cons_peak = 0.0

    def refresh(self, phase, sent, sent_bytes, stats, shared, alive):
        now = time.monotonic()
        delivered = stats.delivered
        consumed = shared.consumed.value
        nbytes = shared.nbytes.value

        if self.prev is None:
            self.prev = (now, delivered, consumed, nbytes)
        elif now - self.prev[0] >= 0.25:
            pt, pd, pc, pb = self.prev
            dt = now - pt
            self.prod_rate = (delivered - pd) / dt
            self.cons_rate = (consumed - pc) / dt
            avg_len = sent_bytes / sent if sent else 0
            self.prod_mbps = self.prod_rate * avg_len / 1e6
            self.cons_mbps = (nbytes - pb) / dt / 1e6
            self.prod_hist.append(self.prod_rate)
            self.cons_hist.append(self.cons_rate)
            self.prod_peak = max(self.prod_peak, self.prod_rate)
            self.cons_peak = max(self.cons_peak, self.cons_rate)
            self.prev = (now, delivered, consumed, nbytes)

        elapsed = now - self.t0
        lag = max(0, delivered - consumed)

        if not self.tty:  # piped output: one plain line per second
            if now - self.last_plain >= 1.0:
                self.last_plain = now
                print(f"[{elapsed:5.1f}s] {phase}: produced={delivered:,} "
                      f"ingested={consumed:,} lag={lag:,} "
                      f"produce={self.prod_rate:,.0f}/s ingest={self.cons_rate:,.0f}/s",
                      flush=True)
            return

        tally = shared.by_type[:]
        total = sum(tally) or 1
        warn = ""
        if alive < self.n_consumers:
            warn = f"{RED}! only {alive}/{self.n_consumers} consumer processes alive{RESET}"
        a = self.args
        # Keep every line under 80 visible columns so the in-place redraw never wraps.
        L = [
            f"{BOLD}{CYAN}KAFKA HIGH-VELOCITY INGESTION DEMO{RESET}   {YELLOW}{phase}{RESET}",
            f"{DIM}{a.bootstrap} | {a.partitions} partitions | "
            f"{self.n_consumers} consumers | ~{a.size} B/event | acks={a.acks}{RESET}",
            warn,
            f"elapsed   {int(elapsed // 60):02d}:{elapsed % 60:04.1f}",
            f"produced  {GREEN}{delivered:>14,}{RESET} events acked by the broker",
            f"ingested  {GREEN}{consumed:>14,}{RESET} events consumed and parsed",
            f"lag       {YELLOW}{lag:>14,}{RESET} events waiting in Kafka",
            "",
            f"produce   {BOLD}{self.prod_rate:>10,.0f}{RESET} evt/s  "
            f"{CYAN}{spark(self.prod_hist)}{RESET}  peak {self.prod_peak:,.0f}",
            f"ingest    {BOLD}{self.cons_rate:>10,.0f}{RESET} evt/s  "
            f"{CYAN}{spark(self.cons_hist)}{RESET}  peak {self.cons_peak:,.0f}",
            f"data      {self.prod_mbps:>6.1f} MB/s in    {self.cons_mbps:>6.1f} MB/s out",
            "",
            f"{DIM}event types (ingested){RESET}",
        ]
        for name, count in zip(TYPES, tally):
            filled = round(24 * count / total)
            bar = "█" * filled + "░" * (24 - filled)
            L.append(f"  {name:<12}{CYAN}{bar}{RESET} {count:>12,}")
        L.append(f"revenue   ${shared.cents.value / 100:>16,.2f} from purchase events")
        self._draw(L)

    def _draw(self, lines):
        out = []
        if self.nlines:
            out.append(f"\x1b[{self.nlines}A")
        for ln in lines:
            out.append("\r\x1b[K" + ln + "\n")
        sys.stdout.write("".join(out))
        sys.stdout.flush()
        self.nlines = len(lines)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Kafka high-velocity ingestion demo")
    p.add_argument("--bootstrap", default="localhost:9092", help="broker address")
    p.add_argument("--duration", type=float, default=20, help="seconds to produce (default 20)")
    p.add_argument("--max-messages", type=int, default=0, help="stop after N events (0 = no cap)")
    p.add_argument("--partitions", type=int, default=6, help="topic partitions (default 6)")
    p.add_argument("--consumers", type=int, default=2, help="consumer processes (default 2)")
    p.add_argument("--size", type=int, default=256, help="approx bytes per event (default 256)")
    p.add_argument("--acks", default="1", choices=["0", "1", "all"], help="producer acks (default 1)")
    p.add_argument("--connect-timeout", type=float, default=20, help="seconds to wait for the broker")
    p.add_argument("--keep-topic", action="store_true", help="don't delete the topic afterwards")
    return p.parse_args()


def wait_for_broker(admin, bootstrap, timeout):
    deadline = time.monotonic() + timeout
    print(f"Connecting to Kafka at {bootstrap} ", end="", flush=True)
    while True:
        try:
            admin.list_topics(timeout=3)
            print(" connected.")
            return
        except Exception:
            if time.monotonic() > deadline:
                break
            print(".", end="", flush=True)
    sys.exit(
        f"\n\nCan't reach Kafka at {bootstrap}.\n"
        "  Start it with:   docker compose up -d\n"
        "  Give it ~10 seconds to boot, then run this again."
    )


def delete_topic(admin, topic):
    try:
        admin.delete_topics([topic], operation_timeout=10)[topic].result(timeout=15)
    except Exception:
        pass


def main():
    args = parse_args()
    if os.name == "nt":
        os.system("")  # turns on ANSI escape handling in Windows consoles
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    n_cons = max(1, min(args.consumers, args.partitions))
    if n_cons != args.consumers:
        print(f"Using {n_cons} consumers (can't have more consumers than partitions).")

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    wait_for_broker(admin, args.bootstrap, args.connect_timeout)

    topic = f"demo-events-{int(time.time())}"
    group = f"demo-group-{topic}"
    try:
        admin.create_topics(
            [NewTopic(topic, num_partitions=args.partitions, replication_factor=1)]
        )[topic].result(timeout=20)
    except Exception as e:
        sys.exit(f"Could not create topic: {e}")

    ctx = mp.get_context("spawn")
    shared = Shared(ctx)
    procs = [
        ctx.Process(target=consumer_worker, args=(args.bootstrap, topic, group, shared), daemon=True)
        for _ in range(n_cons)
    ]
    producer = None
    stats = ProducerStats()
    sent = sent_bytes = 0
    t_start = t_prod_end = t_end = time.monotonic()
    interrupted = False
    dash = Dashboard(args, topic, n_cons)

    try:
        for p in procs:
            p.start()
        print(f"Starting {n_cons} consumers ", end="", flush=True)
        wait_until = time.monotonic() + 30
        while shared.ready.value < n_cons:
            if any(p.exitcode not in (None, 0) for p in procs):
                sys.exit("\nA consumer process crashed on startup.")
            if time.monotonic() > wait_until:
                print("\n(continuing: consumers are slow to join the group)")
                break
            print(".", end="", flush=True)
            time.sleep(0.5)
        print(" ready.\n")

        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "acks": args.acks,
            "linger.ms": 25,
            "batch.size": 1_000_000,
            "batch.num.messages": 50_000,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 2_000_000,
            "queue.buffering.max.kbytes": 1_048_576,
            "enable.idempotence": False,
        })
        p_type, p_user, p_key, p_dev, p_amt = build_pools()
        pad = make_pad(args.size)
        produce = producer.produce
        on_delivery = stats.on_delivery

        if dash.tty:
            sys.stdout.write("\x1b[?25l")  # hide cursor while the dashboard runs
        dash.t0 = t_start = time.monotonic()
        deadline = t_start + args.duration
        last_draw = 0.0
        ts = int(time.time() * 1000)

        # ---- produce phase ------------------------------------------------
        while True:
            if not (sent & 1023):
                producer.poll(0)
                now = time.monotonic()
                if now >= deadline or (args.max_messages and sent >= args.max_messages):
                    break
                ts = int(time.time() * 1000)
                if now - last_draw >= 0.5:
                    last_draw = now
                    dash.refresh("PRODUCING", sent, sent_bytes, stats, shared,
                                 sum(p.is_alive() for p in procs))
            j = sent & MASK
            value = TEMPLATE % (sent, ts, p_type[j], p_user[j], p_dev[j], p_amt[j], pad)
            while True:
                try:
                    produce(topic, value=value, key=p_key[j], on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.05)
            sent += 1
            sent_bytes += len(value)

        # ---- flush outstanding batches ------------------------------------
        t_flush = time.monotonic()
        while len(producer) > 0 and time.monotonic() - t_flush < 30:
            producer.poll(0.1)
            now = time.monotonic()
            if now - last_draw >= 0.5:
                last_draw = now
                dash.refresh("FLUSHING", sent, sent_bytes, stats, shared,
                             sum(p.is_alive() for p in procs))
        t_prod_end = time.monotonic()

        # ---- let consumers drain ------------------------------------------
        last_seen, last_change = -1, time.monotonic()
        while shared.consumed.value < stats.delivered:
            dash.refresh("CONSUMERS CATCHING UP", sent, sent_bytes, stats, shared,
                         sum(p.is_alive() for p in procs))
            time.sleep(0.25)
            now = time.monotonic()
            seen = shared.consumed.value
            if seen != last_seen:
                last_seen, last_change = seen, now
            elif now - last_change > 10:
                break
        t_end = time.monotonic()
        time.sleep(0.3)
        dash.refresh("DONE", sent, sent_bytes, stats, shared, sum(p.is_alive() for p in procs))

    except KeyboardInterrupt:
        interrupted = True
        t_end = time.monotonic()
    finally:
        if dash.tty:
            sys.stdout.write("\x1b[?25h")  # show cursor again
            sys.stdout.flush()
        shared.stop.set()
        for p in procs:
            p.join(timeout=5)
        for p in procs:
            if p.is_alive():
                p.terminate()
        if not args.keep_topic:
            delete_topic(admin, topic)

    # ---- summary ----------------------------------------------------------
    delivered, consumed = stats.delivered, shared.consumed.value
    prod_secs = max(t_prod_end - t_start, 1e-9)
    wall = max(t_end - t_start, 1e-9)
    print()
    print(f"{BOLD}RESULT{RESET}{' (interrupted)' if interrupted else ''}")
    print(f"  produced   {delivered:>12,} events in {prod_secs:.1f}s"
          f"  ->  avg {delivered / prod_secs:,.0f} evt/s, peak {dash.prod_peak:,.0f} evt/s")
    print(f"  ingested   {consumed:>12,} events end-to-end in {wall:.1f}s"
          f"  ->  avg {consumed / wall:,.0f} evt/s")
    print(f"  data       {shared.nbytes.value / 1e6:>10,.0f} MB consumed"
          f"  ->  avg {shared.nbytes.value / 1e6 / wall:,.1f} MB/s")
    if stats.failed or shared.errors.value:
        print(f"  {RED}failures: {stats.failed:,} undelivered, {shared.errors.value:,} consumer errors{RESET}")
    if consumed < delivered and not interrupted:
        print(f"  {YELLOW}{delivered - consumed:,} events were still in flight when the demo stopped{RESET}")
    if not args.keep_topic:
        print(f"{DIM}  (topic {topic} deleted){RESET}")


if __name__ == "__main__":
    main()
