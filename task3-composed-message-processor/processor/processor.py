import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='composite_requests', durable=True)
channel.queue_declare(queue='q_drilling', durable=True)
channel.queue_declare(queue='q_electrical', durable=True)
channel.queue_declare(queue='q_logistics', durable=True)

def callback(ch, method, properties, body):
    request = json.loads(body)
    request_id = request['request_id']
    well_id = request['well_id']
    
    print(f" [>] Процессор: Получена заявка {request_id}. Разбиваем на части...")

    # Разделитель (Splitter) + Маршрутизатор (Router)
    for work in request['works']:
        split_message = {
            "request_id": request_id,
            "well_id": well_id,
            "work_type": work
        }
        
        # Маршрутизация по типу работ
        routing_key = f"q_{work}"
        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=json.dumps(split_message).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"     -> Отправлено в {routing_key}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='composite_requests', on_message_callback=callback)
print(" [*] Процессор (Splitter+Router) запущен.")
channel.start_consuming()