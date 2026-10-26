import pika
import json
import time
import uuid

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

# Объявляем exchange (обменник) с типом 'fanout'. Это ключевой момент для паттерна Publish/Subscribe.
channel.exchange_declare(exchange='price_news', exchange_type='fanout')

# Включаем publisher confirms (Guaranteed Delivery)
channel.confirm_delivery()

# Даем подписчикам время создать свои очереди и привязаться к обменнику
print(" [*] Ждем 5 секунд, пока подписчики подключатся...")
time.sleep(5)

# Имитируем публикацию новости об изменении цены
price = 75.50
message_id = str(uuid.uuid4())  # Correlation Identifier
message = {
    "message_id": message_id,
    "symbol": "BRENT",
    "price": price,
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
}

try:
    channel.basic_publish(
        exchange='price_news',
        routing_key='',
        body=json.dumps(message).encode('utf-8'),
        properties=pika.BasicProperties(
            delivery_mode=2,        # Persistent
            message_id=message_id   # Correlation ID в заголовке
        ),
        mandatory=True
    )
    print(f" [x] Опубликована новость: цена на нефть {price}")
    print(f"     Correlation ID: {message_id}")
    print(f"     Подтверждение от брокера получено (сообщение на диске).")
except pika.exceptions.UnroutableError:
    print(" [!] ОШИБКА: сообщение не может быть доставлено ни в одну очередь!")

connection.close()