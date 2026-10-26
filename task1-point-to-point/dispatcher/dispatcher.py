import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='invalid_requests', durable=True)

def callback(ch, method, properties, body):
    data = json.loads(body)
    original = data.get('original_message', {})
    error = data.get('error', 'Неизвестная ошибка')
    rejected_by = data.get('rejected_by', 'Неизвестно')

    print("=" * 60)
    print(f" [!] НЕВАЛИДНАЯ ЗАЯВКА (отклонил: {rejected_by})")
    print(f"     Причина: {error}")
    print(f"     Содержимое: {original}")
    print(f"     Требуется ручной разбор администратором.")
    print("=" * 60)

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='invalid_requests', on_message_callback=callback)

print(" [*] Диспетчер невалидных заявок запущен и ждет сообщений.")
channel.start_consuming()