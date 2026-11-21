import json
import os
import random
import time
from datetime import datetime, timezone

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")

# id задач, которые уже обработали (защита от дублей)
processed = {}


def get_connection():
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        except Exception as e:
            print("RabbitMQ недоступен, ждём...", e, flush=True)
            time.sleep(2)


def publish_result(ch, job_id, result, completed_at):
    event = {"id": job_id, "result": result, "completedAt": completed_at}
    ch.basic_publish(
        exchange="",
        routing_key="results",
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print("Опубликовано JobCompleted:", event, flush=True)


def callback(ch, method, properties, body):
    task = json.loads(body)
    job_id = task["id"]
    print("Получена задача:", task, flush=True)

    # если такую задачу уже делали - заново не обрабатываем
    if job_id in processed:
        print("Дубль, задача уже обработана:", job_id, flush=True)
        result, completed_at = processed[job_id]
        publish_result(ch, job_id, result, completed_at)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # "медленная" обработка
    time.sleep(random.uniform(2, 3))
    result = task["text"].upper()
    completed_at = datetime.now(timezone.utc).isoformat()
    print("Задача обработана:", job_id, "->", result, flush=True)

    processed[job_id] = (result, completed_at)
    publish_result(ch, job_id, result, completed_at)

    # ack только после того как всё сделали
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue="tasks", durable=True)
    channel.queue_declare(queue="results", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="tasks", on_message_callback=callback)
    print("Сервис B слушает очередь tasks", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
