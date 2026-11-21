# Асинхронное взаимодействие через RabbitMQ

Два сервиса общаются только через очереди RabbitMQ:

- **Сервис A** (FastAPI, порт 8000) принимает задачи по HTTP, кладёт их в очередь `tasks` и слушает очередь `results`.
- **Сервис B** (Python + pika) слушает `tasks`, «медленно» обрабатывает задачу (спит 2–3 сек и переводит текст в верхний регистр) и публикует событие `JobCompleted(id, result, completedAt)` в очередь `results`.
- **RabbitMQ** с management UI.

```
client --HTTP--> [A] --tasks--> [RabbitMQ] --tasks--> [B]
client <--HTTP-- [A] <--results-- [RabbitMQ] <--results-- [B]
```

## Запуск

Нужны Docker и Docker Compose. Из корня проекта:

```bash
docker compose up --build
```

Поднимаются три контейнера: `rabbitmq`, `service-a`, `service-b`. Сервисы ждут, пока RabbitMQ станет healthy, поэтому первый запуск занимает около 10–20 сек.

## Как пользоваться (curl)

### 1. Создать задачу

```bash
curl -i -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"text": "hello rabbit"}'
```

Ответ приходит сразу:

```
HTTP/1.1 202 Accepted
content-type: application/json

{"id":"3f2b9c1e-...","status":"ACCEPTED"}
```

### 2. Сразу проверить статус

```bash
curl http://localhost:8000/jobs/<id>
```

```json
{"id":"3f2b9c1e-...","status":"ACCEPTED","text":"hello rabbit","result":null}
```

### 3. Проверить через ~3 сек

```bash
sleep 3
curl http://localhost:8000/jobs/<id>
```

```json
{"id":"3f2b9c1e-...","status":"DONE","text":"hello rabbit","result":"HELLO RABBIT","completedAt":"2026-11-21T12:00:03.123456+00:00"}
```

Всё одной командой (нужен `jq`):

```bash
ID=$(curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{"text":"hello rabbit"}' | jq -r .id)
curl -s http://localhost:8000/jobs/$ID; echo
sleep 3
curl -s http://localhost:8000/jobs/$ID; echo
```

Если id не существует, GET вернёт `404 {"detail":"Job not found"}`.

Логи сервиса B:

```bash
docker compose logs -f service-b
```

```
Получена задача: {'id': '3f2b...', 'text': 'hello rabbit'}
Задача обработана: 3f2b... -> HELLO RABBIT
Опубликовано JobCompleted: {'id': '3f2b...', 'result': 'HELLO RABBIT', 'completedAt': '...'}
```

## Management UI

Адрес: http://localhost:15672, логин `guest`, пароль `guest`.

Очереди находятся на вкладке **Queues and Streams**:

- `tasks`: задачи от A к B (публикует A, читает B);
- `results`: события `JobCompleted` от B к A (публикует B, читает A).

Обе очереди durable. В колонке Consumers у каждой должен быть 1 потребитель. Если быстро отправить несколько POST, видно, как сообщения копятся в `tasks` (Ready / Unacked), а потом разбираются.

## Почему POST отвечает 202, а не 200

`202 Accepted` значит «запрос принят, но ещё не выполнен». Сервис A не ждёт обработки: он только кладёт задачу в очередь и сразу возвращает id. Результат появится позже, и его можно узнать через `GET /jobs/{id}`. Код 200 означал бы, что работа уже сделана, а это не так.

## Что будет, если B упадёт посреди обработки

Сервис B использует **ручной ack**: `basic_ack` отправляется только после того, как задача обработана и событие опубликовано в `results`. Ещё стоит `prefetch_count=1`, то есть B берёт по одной задаче за раз.

- Если B упал до ack (во время `sleep` или сразу после публикации результата), RabbitMQ видит, что соединение закрылось, а сообщение не подтверждено, и возвращает его в очередь `tasks`. После перезапуска B получит задачу снова.
- Очереди durable, а сообщения persistent (`delivery_mode=2`), поэтому они переживут и перезапуск RabbitMQ.

Это гарантия **at-least-once** (хотя бы один раз): задача не потеряется, но может обработаться **повторно**.

Проверить можно так:

```bash
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{"text":"crash test"}'
docker compose kill service-b      # сразу, пока идёт sleep
# в UI у очереди tasks сообщение снова в Ready
docker compose start service-b
docker compose logs service-b      # задача получена ещё раз и обработана
```

## Защита от дублей

Раз доставка at-least-once, одна и та же задача может прийти несколько раз. Защиты две:

1. **В сервисе B** есть словарь `processed` с id уже обработанных задач. Если пришла задача с известным id, B не обрабатывает её заново, а просто ещё раз публикует сохранённый результат и делает ack.
   Ограничение: словарь хранится в памяти, поэтому после перезапуска B он пустой. Если B упал после обработки, но до ack, после рестарта задача обработается ещё раз. В реальном проекте `processed` хранили бы в Redis или в БД.
2. **В сервисе A** обработка `JobCompleted` идемпотентна: она просто ставит `status = DONE` и записывает результат по id. Если одно и то же событие придёт дважды, состояние не изменится.

## Структура

```
docker-compose.yml
service_a/   - FastAPI, POST /jobs, GET /jobs/{id}, слушает results
service_b/   - воркер, слушает tasks, публикует в results
```
