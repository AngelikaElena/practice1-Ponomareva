import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='q_aggregator', durable=True)
channel.queue_declare(queue='final_estimates', durable=True)

# База данных в памяти для хранения состояния
aggregates = {}

def callback(ch, method, properties, body):
    response = json.loads(body)
    req_id = response['request_id']
    
    print(f" [A] Агрегатор получил расчет по '{response['work_type']}' для ID: {req_id}")

    # Инициализируем запись, если ее нет
    if req_id not in aggregates:
        aggregates[req_id] = {
            "well_id": response['well_id'],
            "details": []
        }
    
    # Добавляем детали
    aggregates[req_id]["details"].append({
        "work": response['work_type'],
        "cost": response['cost'],
        "time": response['time']
    })
    
    # Проверяем, все ли 3 части собраны
    if len(aggregates[req_id]["details"]) == 3:
        print(f" [!] Агрегатор: Все части для {req_id} собраны! Формируем итоговую смету.")
        
        final = aggregates[req_id]
        final["request_id"] = req_id
        final["total_cost"] = sum(d['cost'] for d in final['details'])
        final["total_time"] = max(d['time'] for d in final['details']) # Предположим, работы идут параллельно
        
        # Отправляем инициатору
        channel.basic_publish(
            exchange='',
            routing_key='final_estimates',
            body=json.dumps(final).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        # Удаляем из памяти
        del aggregates[req_id]

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='q_aggregator', on_message_callback=callback)
print(" [*] Агрегатор запущен.")
channel.start_consuming()