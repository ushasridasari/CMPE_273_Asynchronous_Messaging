"""Order Worker: competing consumer with manual ACK and idempotent handling."""
import argparse
import fcntl
import json
import os
import time

import pika

QUEUE = "orders"
PROCESSED_FILE = "processed_events.txt"
LOCK_FILE = "processed_events.lock"


def claim_event(event_id):
    """Atomically record event_id in a file shared by all workers.
    Returns True if new, False if it was already processed."""
    with open(LOCK_FILE, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        seen = set()
        if os.path.exists(PROCESSED_FILE):
            with open(PROCESSED_FILE) as f:
                seen = {line.strip() for line in f}
        if event_id in seen:
            return False
        with open(PROCESSED_FILE, "a") as f:
            f.write(event_id + "\n")
        return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", default=f"worker-{os.getpid()}")
    p.add_argument("--host", default="localhost")
    p.add_argument("--delay", type=float, default=2.0, help="simulated work time (s)")
    p.add_argument("--crash-on", default=None, metavar="ORDER_ID",
                   help="exit WITHOUT ack the first time this order_id arrives")
    args = p.parse_args()

    def log(msg):
        print(f"[{time.strftime('%H:%M:%S')}] [{args.name}] {msg}", flush=True)

    def on_message(ch, method, properties, body):
        order = json.loads(body)
        tag = " (REDELIVERED)" if method.redelivered else ""
        log(f"received order {order['order_id']} event_id={order['event_id']}{tag}")

        # Failure simulation: die after receiving, before ACK
        if args.crash_on == order["order_id"] and not method.redelivered:
            log(f"!!! simulating crash on {order['order_id']} before ACK")
            os._exit(1)

        if not claim_event(order["event_id"]):
            log(f"DUPLICATE detected for event_id={order['event_id']} "
                f"(order {order['order_id']}) - skipping business action")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        log(f"processing order {order['order_id']}: "
            f"{order['quantity']} x {order['item_id']}")
        time.sleep(args.delay)  # business action
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log(f"ACK sent for order {order['order_id']}")

    conn = pika.BlockingConnection(pika.ConnectionParameters(args.host))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)  # fair dispatch: one unacked message at a time
    ch.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
    log("waiting for messages. Ctrl+C to exit.")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
