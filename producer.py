"""Order Producer: publishes order messages to a durable RabbitMQ queue."""
import argparse
import json
import uuid

import pika

QUEUE = "orders"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--count", type=int, default=6, help="number of orders to publish")
    p.add_argument("--duplicate", type=int, default=0, metavar="N",
                   help="re-publish the Nth order again with the SAME event_id")
    args = p.parse_args()

    conn = pika.BlockingConnection(pika.ConnectionParameters(args.host))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)

    messages = []
    for i in range(1, args.count + 1):
        messages.append({
            "event_id": str(uuid.uuid4()),
            "order_id": f"ORD-{i}",
            "item_id": f"ITEM-{100 + i}",
            "quantity": i,
        })
    if args.duplicate:
        messages.append(dict(messages[args.duplicate - 1]))  # identical event_id

    for msg in messages:
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(msg),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        print(f"[producer] published {msg}")

    conn.close()


if __name__ == "__main__":
    main()
