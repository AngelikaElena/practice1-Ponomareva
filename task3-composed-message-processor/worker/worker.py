import pika
import json
import os
import time
import random

work_type = os.environ.get('WORK_TYPE', 'unknown')
queue_name = f'q_{work_type}'

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue=queue_name, durable=True)
channel.queue_declare(queue='q_aggregator', durable=True)

def callback(ch, method, properties, body):
    task = json.loads(body)
    request_id = task['request_id']
    
    print(f" [W] Исполнитель '{work_type}': Получена задача для {task['well_id']} (ID: {request_id})")
    
    # Имитация расчета сметы
    time.sleep(1)
    cost = random.randint(10000, 50000)
    days = random.randint(1, 10)
    
    response = {
        "request_id": request_id,
        "well_id": task['well_id'],
        "work_type": work_type,
        "cost": cost,
        "time": days
    }
    
    # Отправляем ответ Агрегатору
    channel.basic_publish(
        exchange='',
        routing_key='q_aggregator',
        body=json.dumps(response).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f"     <- Отправлен расчет в агрегатор: {cost} руб., {days} дн.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=queue_name, on_message_callback=callback)
print(f" [*] Исполнитель '{work_type}' запущен и ждет задач.")
channel.start_consuming()