import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = "energy_usage"
BOOTSTRAP = "localhost:9092"
SLEEP_SEC = 15

HOUSEHOLDS = ["H001", "H002", "H003", "H004", "H005", "H006", "H007", "H008"]


def generate_energy_data():
    current_time = datetime.now()
    household_id = random.choice(HOUSEHOLDS)

    if random.random() < 0.05:
        energy_kwh = round(random.uniform(1.5, 3.0), 2)
    else:
        energy_kwh = round(random.uniform(0.1, 0.8), 2)

    return {
        "timestamp": current_time.isoformat(),
        "household_id": household_id,
        "energy_kwh": energy_kwh
    }


def check_kafka_connection():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        return result == 0
    except:
        return False


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 0, 2),
            retries=5,
            request_timeout_ms=30000,
            security_protocol="PLAINTEXT"
        )
        logger.info(f"Połączono z Kafka pod adresem: {BOOTSTRAP}")
        return producer
    except Exception as e:
        logger.error(f"Błąd podczas łączenia z Kafka: {e}")
        return None


def main():
    logger.info("Próba uruchomienia producenta danych...")

    if not check_kafka_connection():
        logger.error("Port 9092 nie jest dostępny! Upewnij się że Kafka jest uruchomiona.")
        logger.error("Uruchom: docker-compose -f docker-compose-all.yml up -d")
        return

    max_retries = 10
    retry_delay = 3

    for attempt in range(max_retries):
        producer = create_producer()
        if producer:
            break
        logger.warning(f"Próba {attempt + 1}/{max_retries} - Oczekiwanie na Kafka...")
        time.sleep(retry_delay)
    else:
        logger.error("Nie udało się połączyć z Kafka po wszystkich próbach")
        return

    print("Rozpoczęcie wysyłania danych o zużyciu energii...")
    print("Format: timestamp, household_id, energy_kwh")
    print("Wysyłanie co 15 sekund...")
    print("Naciśnij Ctrl+C aby zatrzymać")

    # Wysyłanie danych
    try:
        message_count = 0
        while True:
            data = generate_energy_data()
            try:
                future = producer.send(TOPIC, value=data)
                # Czekaj na potwierdzenie wysłania
                future.get(timeout=10)
                producer.flush()
                message_count += 1
                print(
                    f"→ #{message_count} Wysłano: {data['timestamp']}, {data['household_id']}, {data['energy_kwh']} kWh")
                time.sleep(SLEEP_SEC)
            except Exception as e:
                logger.error(f"Błąd podczas wysyłania wiadomości: {e}")
                time.sleep(5)

    except KeyboardInterrupt:
        print("\nZatrzymywanie producenta...")
    finally:
        producer.close()
        print("Producer zamknięty.")


if __name__ == "__main__":
    main()