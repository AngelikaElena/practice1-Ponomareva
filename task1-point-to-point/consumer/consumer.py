import pika
import json
import os
import time

# Получаем имя бригады из переменной окружения (например, BRIGADE_NAME=Бригада-1)
brigade_name = os.environ.get('BRIGADE_NAME', 'Неизвестная бригада')

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='repair_requests', durable=True)
channel.queue_declare(queue='invalid_requests', durable=True)

def validate_message(message):
    """Проверяет, что в заявке есть все обязательные поля."""
    if 'request_id' not in message:
        return False, "Отсутствует поле request_id"
    if 'well_id' not in message:
        return False, "Отсутствует поле well_id"
    if not str(message['well_id']).startswith('WELL-'):
        return False, f"Некорректный формат well_id: {message['well_id']}"
    return True, None

def callback(ch, method, properties, body):
    message = json.loads(body)
    request_id = message.get('request_id', 'UNKNOWN')

    is_valid, error = validate_message(message)

    if not is_valid:
        print(f" [{brigade_name}] ОТКЛОНЕНА заявка {request_id}: {error}")
        # Отправляем в Invalid Message Channel
        ch.basic_publish(
            exchange='',
            routing_key='invalid_requests',
            body=json.dumps({
                'original_message': message,
                'error': error,
                'rejected_by': brigade_name
            }).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f" [{brigade_name}] Получена заявка {request_id} на скважину {message['well_id']}.")
    print(f" [{brigade_name}] Начинаю выполнение...")
    time.sleep(2)
    print(f" [{brigade_name}] Заявка {request_id} ВЫПОЛНЕНА.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='repair_requests', on_message_callback=callback)

print(f" [*] {brigade_name} ждет заявок. Для выхода нажмите CTRL+C")
channel.start_consuming()