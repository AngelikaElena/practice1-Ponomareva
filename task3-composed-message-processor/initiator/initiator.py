import pika
import json
import time
import uuid

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='composite_requests', durable=True)
channel.queue_declare(queue='final_estimates', durable=True)

request_id = str(uuid.uuid4())
composite_request = {
    "request_id": request_id,
    "well_id": "WELL-777",
    "works": ["drilling", "electrical", "logistics"]
}

channel.basic_publish(
    exchange='',
    routing_key='composite_requests',
    body=json.dumps(composite_request).encode('utf-8'),
    properties=pika.BasicProperties(delivery_mode=2)
)
print(f" [x] Инициатор: Отправлена общая заявка {request_id} на скважину WELL-777")
print(" [*] Инициатор: Ожидание сводной сметы...")

def callback(ch, method, properties, body):
    estimate = json.loads(body)
    if estimate.get("request_id") == request_id:
        print("\n" + "="*50)
        print(f" [✓] Инициатор получил СВОДНУЮ СМЕТУ по заявке {request_id}:")
        print(f"     Скважина: {estimate.get('well_id')}")
        print(f"     Общая стоимость: {estimate.get('total_cost')}")
        print(f"     Общее время: {estimate.get('total_time')} дней")
        print("     Детали:")
        for detail in estimate.get('details', []):
            print(f"       - {detail['work']}: {detail['cost']} руб. ({detail['time']} дн.)")
        print("="*50 + "\n")
        
        # Подтверждаем обработку и завершаем работу
        ch.basic_ack(delivery_tag=method.delivery_tag)
        connection.close()
        exit(0)

channel.basic_consume(queue='final_estimates', on_message_callback=callback, auto_ack=False)
channel.start_consuming()