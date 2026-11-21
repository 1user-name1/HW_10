import json
import os
import threading
import time
import uuid

import pika
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")

app = FastAPI()

# состояние храним просто в памяти
jobs = {}


class JobRequest(BaseModel):
    text: str


def get_connection():
    # ждём пока RabbitMQ поднимется
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        except Exception as e:
            print("RabbitMQ недоступен, ждём...", e, flush=True)
            time.sleep(2)


def listen_results():
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue="results", durable=True)

    def callback(ch, method, properties, body):
        event = json.loads(body)
        print("Получено событие JobCompleted:", event, flush=True)
        job_id = event["id"]
        if job_id in jobs:
            jobs[job_id]["status"] = "DONE"
            jobs[job_id]["result"] = event["result"]
            jobs[job_id]["completedAt"] = event["completedAt"]
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue="results", on_message_callback=callback)
    print("Сервис A слушает очередь results", flush=True)
    channel.start_consuming()


@app.on_event("startup")
def startup():
    t = threading.Thread(target=listen_results, daemon=True)
    t.start()


@app.post("/jobs", status_code=202)
def create_job(req: JobRequest):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"id": job_id, "status": "ACCEPTED", "text": req.text, "result": None}

    # pika не потокобезопасна, поэтому на каждую публикацию своё соединение
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue="tasks", durable=True)
    channel.basic_publish(
        exchange="",
        routing_key="tasks",
        body=json.dumps({"id": job_id, "text": req.text}),
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )
    connection.close()

    print("Задача опубликована в tasks:", job_id, flush=True)
    return JSONResponse(status_code=202, content={"id": job_id, "status": "ACCEPTED"})


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]
