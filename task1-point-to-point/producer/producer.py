import pika
import json
import time

# Устанавливаем соединение с RabbitMQ. Имя хоста 'rabbitmq' — это имя сервиса из docker-compose.
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

# Объявляем очередь. 'durable=True' гарантирует, что очередь не потеряется при перезапуске RabbitMQ.
channel.queue_declare(queue='repair_requests', durable=True)
# Объявляем очередь для невалидных сообщений (Invalid Message Channel)
channel.queue_declare(queue='invalid_requests', durable=True)

# Включаем publisher confirms (Guaranteed Delivery)
channel.confirm_delivery()

# --- 5 валидных заявок ---
for i in range(1, 6):
    request_id = f"REQ-{i}"
    message = {
        "request_id": request_id,
        "well_id": f"WELL-{100 + i}",
        "description": f"Плановый ремонт скважины {100 + i}"
    }
    try:
        channel.basic_publish(
            exchange='',
            routing_key='repair_requests',
            body=json.dumps(message).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f" [x] Отправлена валидная заявка: {request_id} (подтверждено брокером)")
    except pika.exceptions.UnroutableError:
        print(f" [!] ОШИБКА: заявка {request_id} не доставлена!")
    time.sleep(1)

# --- 2 невалидные заявки (нет well_id) ---
for i in range(6, 8):
    request_id = f"REQ-{i}"
    message = {
        "request_id": request_id,
        "description": f"Заявка без указания скважины"
        # Поля well_id нет — это ошибка
    }
    try:
        channel.basic_publish(
            exchange='',
            routing_key='repair_requests',
            body=json.dumps(message).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f" [x] Отправлена НЕВАЛИДНАЯ заявка: {request_id} (нет well_id)")
    except pika.exceptions.UnroutableError:
        print(f" [!] ОШИБКА: заявка {request_id} не доставлена!")
    time.sleep(1)
        
connection.close()