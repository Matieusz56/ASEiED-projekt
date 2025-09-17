# System Monitorowania Zużycia Energii w Czasie Rzeczywistym

### Autorzy: Mateusz Waszczuk (193666), Grzegorz Macioch (193670)

##  Spis Treści
- [ Cel Projektu](#cel-projektu)
- [ Architektura Systemu](#architektura-systemu)
- [ Wymagane Komponenty](#wymagane-komponenty)
- [ Uruchomienie i Konfiguracja](#uruchomienie-i-konfiguracja)
- [ Weryfikacja Działania](#weryfikacja-działania)
- [ Przetwarzanie Danych](#przetwarzanie-danych)
- [ Wykrywanie Anomalii](#wykrywanie-anomalii)
- [ Output i Wyniki](#output-i-wyniki)
- [ Workflow Systemu](#workflow-systemu)

## Cel Projektu

System monitorowania zużycia energii w gospodarstwach domowych z wykrywaniem anomalii w czasie rzeczywistym. Projekt wykorzystuje Apache Spark Streaming i Apache Kafka do przetwarzania strumieniowego danych.

## Architektura Systemu

```mermaid
graph TB
    subgraph "Data Generation Layer"
        P[Producer<br/>Python Generator<br/>Co 15 sekund]
    end
    
    subgraph "Streaming Platform"
        Z[Zookeeper]
        K[Kafka Broker<br/>energy_usage topic]
        Z --> K
    end
    
    subgraph "Processing Engine"
        SM[Spark Master]
        SW[Spark Worker]
        SS[Spark Streaming Job]
        SM --> SW --> SS
    end
    
    subgraph "Storage Layer"
        CP[Checkpoints]
        AG[Aggregacje CSV]
        AL[Alerty CSV]
    end
    
    subgraph "Monitoring"
        C[Console Output]
        W[Spark Web UI]
    end
    
    P --> K
    K --> SS
    SS --> CP
    SS --> AG
    SS --> AL
    SS --> C
    SM --> W
    
    style P fill:#e1f5fe,color:black
    style K fill:#f3e5f5,color:black
    style SS fill:#fff3e0,color:black
    style AG,AL fill:#e8f5e8,color:black
    style C,W fill:#ffebee,color:black
```
## Wymagane Komponenty
- Docker & Docker Compose

- Apache Kafka (via Docker)

- Apache Spark 3.5.1 (via Docker)

- Python 3.9+ z kafka-python

- Java 8+ (dla Spark)

## Uruchomienie i Konfiguracja

```
#Uruchomienie infrasturktury
docker-compose -f docker-compose-all.yml up -d

#Poczekaj 30s na inicjalizację Kafki

#Uruchom przetwarzanie Spark
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/app/spark_streaming.py
```

## Weryfikacja Działania

```
# Sprawdzenie czy producer wysyła dane
docker logs energy-producer -f

# Sprawdzenie topic Kafka
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitorowanie Spark UI
open http://localhost:8080
```
## Przetwarzanie Danych

### Pipeline Przetwarzania:
#### 1. Odczyt z Kafka - Format binarny:
- Pozyskiwanie surowych danych z topicu Kafka energy_usage
- Dane w początkowym formacie binarnym
- Konfiguracja połączenia z brokerem Kafka

#### 2. Parsowanie JSON:
- Konwersja danych binarnych na string JSON
- Wyodrębnienie pól JSON do strukturyzowanego DataFrame
- Walidacja i czyszczenie danych

#### 3. Watermarking (Obsługa 5-minutowego opóźnienia)

#### 4. Grupowanie danych w 15-minutowe okna czasowe

#### 5. Wykrywanie anomalii (zużycie >2kWh/15min)

#### 6. Zapis agregacji i alertów do plików CSV

### Generowanie wartości zużycia (producer_docker.py):

```
    if random.random() < 0.15:
        energy_kwh = round(random.uniform(2.5, 2), 6) 
    else:
        energy_kwh = round(random.uniform(0.0005, 0.0015), 6) 
```

## Wykrywanie Anomalii

- Progowanie: 2 kWh na 15-minutowe okno
- Typ alertu: HIGH_USAGE
- Czułość: 15% szansy na anomalie w danych testowych

### Przykładowy Alert
```
household_id,window_start,window_end,total_energy_kwh,alert_type
H003,2024-08-28 14:15:00,2024-08-28 14:30:00,0.187,HIGH_USAGE
```
## Output i Wyniki

### Pliki Wyjściowe

```
/output/
├── energy_aggregations/    # Sumy energii co 15 minut
├── alerts/                 # Wykryte anomalie
└── checkpoint/            # Stan przetwarzania Spark
```

## Workflow systemu
```mermaid
sequenceDiagram
    participant P as Producer (Generator Danych)
    participant K as Kafka (Broker Wiadomości)
    participant S as Spark (Przetwarzanie)
    participant C as Konsola (Wyświetlanie)
    participant F as System Plików (Zapis)

    Note left of P: Co 15 sekund
    P->>K: JSON z danymi energii
    K-->>P: Potwierdzenie odbioru
    
    Note right of S: Ciągłe przetwarzanie
    S->>K: Pobierz wiadomości
    K-->>S: Partia danych
    S->>S: Przetwórz i agreguj
    S->>F: Zapisz agregacje
    S->>F: Zapisz alerty (jeśli wystąpiły)
    S->>C: Wyświetl alerty w czasie rzeczywistym
```