import pika
import json
import os

department_name = os.environ.get('DEPARTMENT_NAME', 'Неизвестный отдел')

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.exchange_declare(exchange='price_news', exchange_type='fanout')

# Именованная durable-очередь для каждого отдела
queue_name = f'price_news_{department_name}'
channel.queue_declare(queue=queue_name, durable=True)

channel.queue_bind(exchange='price_news', queue=queue_name)

print(f" [*] {department_name} подписался на новости о ценах (очередь: {queue_name}).")

def callback(ch, method, properties, body):
    message = json.loads(body)
    correlation_id = message.get('message_id', 'N/A')
    print(f" [{department_name}] Получена новость (Correlation ID: {correlation_id}):")
    print(f"     Цена на {message['symbol']} = {message['price']} (время: {message['timestamp']})")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(f" [*] {department_name} ждет новостей. Для выхода нажмите CTRL+C")
channel.start_consuming()