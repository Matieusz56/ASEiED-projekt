import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging
import socket
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = "energy_usage"
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')  # Dla Dockera: kafka:9092
SLEEP_SEC = 15

HOUSEHOLDS = ["H001", "H002", "H003", "H004", "H005", "H006", "H007", "H008"]


def generate_energy_data():
    current_time = datetime.now()
    household_id = random.choice(HOUSEHOLDS)

    if random.random() < 0.15:
        energy_kwh = round(random.uniform(2.5, 2), 6)
    else:
        energy_kwh = round(random.uniform(0.0005, 0.0015), 6)

    return {
        "timestamp": current_time.isoformat(),
        "household_id": household_id,
        "energy_kwh": energy_kwh
    }


def check_kafka_connection(host, port):
    """Sprawdza czy port Kafka jest dostępny"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def create_producer():
    """Tworzy producera Kafka z obsługą błędów"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 0, 2),
            retries=5,
            request_timeout_ms=30000,
            security_protocol="PLAINTEXT",
            # DODAJ KONFIGURACJĘ ROZMIARU WIADOMOŚCI:
            max_request_size=10485760,  # 10MB maksymalnie na request
            compression_type='gzip'  # Kompresja danych
        )
        logger.info(f"Połączono z Kafka pod adresem: {BOOTSTRAP}")
        return producer
    except Exception as e:
        logger.error(f"Błąd podczas łączenia z Kafka: {e}")
        return None


def main():
    logger.info("Próba uruchomienia producenta danych...")

    # Rozdziel host i port do sprawdzenia połączenia
    kafka_host = BOOTSTRAP.split(':')[0]
    kafka_port = int(BOOTSTRAP.split(':')[1]) if ':' in BOOTSTRAP else 9092

    # Sprawdź czy Kafka jest dostępna
    max_retries = 20  # Zwiększ liczbę prób dla Dockera
    retry_delay = 5

    for attempt in range(max_retries):
        if check_kafka_connection(kafka_host, kafka_port):
            logger.info(f"Kafka jest dostępna pod {BOOTSTRAP}")
            break
        logger.warning(f"Próba {attempt + 1}/{max_retries} - Oczekiwanie na Kafka pod {BOOTSTRAP}...")
        time.sleep(retry_delay)
    else:
        logger.error(f"Nie udało się połączyć z Kafka pod {BOOTSTRAP} po wszystkich próbach")
        logger.error("Sprawdź czy:")
        logger.error("1. Kafka kontener jest uruchomiony: docker ps")
        logger.error("2. Kafka jest w sieci Docker: docker network inspect energy-monitoring_default")
        return

    # Teraz spróbuj utworzyć producera
    producer = create_producer()
    if not producer:
        logger.error("Nie udało się utworzyć producenta Kafka")
        return

    print("Rozpoczęcie wysyłania danych o zużyciu energii...")
    print(f"Łączenie z: {BOOTSTRAP}")
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