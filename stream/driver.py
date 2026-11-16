import json
import os
import sys

import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/%2F"
)

EXCHANGE = "rapido-dispatch"


def main():
    if len(sys.argv) != 3:
        print("Usage: python stream/driver.py NAME BINDING_PATTERN")
        sys.exit(1)

    driver_name = sys.argv[1]
    binding_pattern = sys.argv[2]

    connection = pika.BlockingConnection(
        pika.URLParameters(RABBITMQ_URL)
    )
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True,
    )

    result = channel.queue_declare(
        queue="",
        exclusive=True,
        auto_delete=True,
    )
    queue_name = result.method.queue

    channel.queue_bind(
        exchange=EXCHANGE,
        queue=queue_name,
        routing_key=binding_pattern,
    )

    print(f"[{driver_name}] Connected")
    print(f"[{driver_name}] Binding: {binding_pattern}")
    print(f"[{driver_name}] Private queue: {queue_name}")
    print(f"[{driver_name}] Waiting for requests...\n")

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))

            if not isinstance(message, dict) or not message.get("zone"):
                print(
                    f"[{driver_name}] REJECTED "
                    f"key={method.routing_key} "
                    f"id={message.get('id') if isinstance(message, dict) else 'unknown'} "
                    f"reason=missing zone | NACK requeue=False"
                )
                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False,
                )
                return

            print(
                f"[{driver_name}] RECEIVED "
                f"key={method.routing_key} "
                f"id={message.get('id')} "
                f"city={message.get('city')} "
                f"zone={message.get('zone')} "
                f"type={message.get('type')} | ACK"
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(
                f"[{driver_name}] REJECTED invalid JSON: {exc} "
                "| NACK requeue=False"
            )
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False,
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[{driver_name}] Stopping...")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()