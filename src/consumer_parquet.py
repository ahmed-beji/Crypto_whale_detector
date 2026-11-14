import os
import io
import json
import time
import signal
import sys
import boto3
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from botocore.exceptions import BotoCoreError, ClientError

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "binance-trades")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")

WHALE_THRESHOLD_USDT = float(os.getenv("WHALE_THRESHOLD_USDT", "50000"))
BATCH_SIZE = int(os.getenv("PARQUET_BATCH_SIZE", "100"))
FLUSH_INTERVAL_SEC = int(os.getenv("PARQUET_FLUSH_SEC", "900"))  # 15 minutes max

if not S3_BUCKET:
    print("❌ ERREUR CRITIQUE : La variable S3_BUCKET_NAME est absente du fichier .env")
    sys.exit(1)

s3_client = boto3.client("s3", region_name=AWS_REGION)

def create_consumer():
    print(f"🔌 Tentative de connexion (confluent-kafka) au broker {KAFKA_BROKER}...")
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'cold_path_s3_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([TOPIC_NAME])
    print(f"✅ Connecté et abonné au topic : {TOPIC_NAME}")
    return consumer


def flush_to_s3(batch_data, consumer):
    """Convertit le lot en Parquet en RAM, l'envoie sur S3, puis commit l'offset Kafka."""
    if not batch_data:
        return True

    now = datetime.now(timezone.utc)
    s3_key = (
        f"whales/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/"
        f"whales_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    try:
        df = pd.DataFrame(batch_data)

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
        parquet_buffer.seek(0)

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )

        # Validation asynchrone pour ne pas bloquer le thread principal
        consumer.commit(asynchronous=True)
        print(f"✅ [S3 UPLOAD & COMMIT] {len(batch_data)} baleines -> s3://{S3_BUCKET}/{s3_key}")
        return True

    except (BotoCoreError, ClientError) as e:
        print(f"❌ [ERREUR S3] Échec de l'upload : {e}")
        return False
    except Exception as e:
        print(f"❌ [ERREUR INATTENDUE] : {e}")
        return False


def main():
    print(f"❄️ [VOIE FROIDE S3] Démarrage | Bucket: {S3_BUCKET} | Batch: {BATCH_SIZE} ou {FLUSH_INTERVAL_SEC}s")
    consumer = create_consumer()
    batch_data = []
    last_flush_time = time.time()

    def graceful_shutdown(signum, frame):
        print("\n🛑 Signal d'arrêt reçu. Sauvegarde d'urgence...")
        flush_to_s3(batch_data, consumer)
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    while True:
        # Lecture par lots avec un timeout strict de 5 secondes
        messages = consumer.consume(num_messages=50, timeout=5.0)

        if messages:
            print(f"📥 {len(messages)} trades lus dans Kafka...")

        for msg in messages:
            if msg.error():
                # Ignore les messages de fin de partition (normal)
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"⚠️ Erreur Kafka : {msg.error()}")
                continue
            
            try:
                # confluent-kafka retourne des bytes, il faut décoder
                trade = json.loads(msg.value().decode("utf-8"))
                price = float(trade.get("p", 0))
                quantity = float(trade.get("q", 0))
                trade_timestamp = int(trade.get("T", 0))
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
            
            value_usdt = price * quantity

            if value_usdt >= WHALE_THRESHOLD_USDT:
                batch_data.append({
                    "trade_timestamp": trade_timestamp,
                    "price": price,
                    "quantity": quantity,
                    "value_usdt": value_usdt,
                    "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat()
                })

        time_elapsed = time.time() - last_flush_time
        if len(batch_data) >= BATCH_SIZE or (len(batch_data) > 0 and time_elapsed >= FLUSH_INTERVAL_SEC):
            reason = "BATCH_SIZE atteint" if len(batch_data) >= BATCH_SIZE else "TIMEOUT écoulé"
            print(f"⏳ Déclenchement du flush ({reason} : {len(batch_data)} baleines)...")

            if flush_to_s3(batch_data, consumer):
                batch_data.clear()
                last_flush_time = time.time()

if __name__ == "__main__":
    main()