import os
import io
import json
import time
import signal
import sys
import boto3
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaConsumer
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

# Initialisation du client S3 (utilise automatiquement l'IAM Instance Profile de l'EC2)
s3_client = boto3.client("s3", region_name=AWS_REGION)


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Commit manuel strict après upload S3
        group_id="cold_path_s3_group"
    )


def flush_to_s3(batch_data, consumer):
    """Convertit le lot en Parquet en RAM, l'envoie sur S3, puis commit l'offset Kafka."""
    if not batch_data:
        return True

    now = datetime.now(timezone.utc)
    # Partitionnement standard Hive pour AWS Athena / Spark / BigQuery
    s3_key = (
        f"whales/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/"
        f"whales_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    try:
        df = pd.DataFrame(batch_data)

        # Écriture du fichier Parquet en mémoire vive (RAM) sans toucher au disque EC2
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
        parquet_buffer.seek(0)

        # Upload vers AWS S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )

        # Validation des offsets Kafka UNIQUEMENT si S3 a confirmé l'écriture
        consumer.commit()
        print(f"✅ [S3 UPLOAD & COMMIT] {len(batch_data)} baleines -> s3://{S3_BUCKET}/{s3_key}")
        return True

    except (BotoCoreError, ClientError) as e:
        print(f"❌ [ERREUR S3] Échec de l'upload, offsets non commités (nouvel essai au prochain cycle) : {e}")
        return False
    except Exception as e:
        print(f"❌ [ERREUR INATTENDUE] : {e}")
        return False


def main():
    print(f"❄️ [VOIE FROIDE S3] Démarrage | Bucket: {S3_BUCKET} | Batch: {BATCH_SIZE} ou {FLUSH_INTERVAL_SEC}s")
    consumer = create_consumer()
    batch_data = []
    last_flush_time = time.time()

    # Gestion de l'arrêt propre (SIGTERM lors d'un docker stop/restart)
    def graceful_shutdown(signum, frame):
        print("\n🛑 Signal d'arrêt reçu. Sauvegarde d'urgence du lot en cours vers S3...")
        flush_to_s3(batch_data, consumer)
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    while True:
        # Poll avec timeout de 5 secondes pour vérifier régulièrement le timer même sans nouveaux messages
        records = consumer.poll(timeout_ms=5000, max_records=50)

        for topic_partition, messages in records.items():
            for message in messages:
                trade = message.value
                value_usdt = float(trade.get("value_usdt", 0))

                if value_usdt >= WHALE_THRESHOLD_USDT:
                    batch_data.append({
                        "trade_timestamp": int(trade["trade_timestamp"]),
                        "price": float(trade["price"]),
                        "quantity": float(trade["quantity"]),
                        "value_usdt": value_usdt,
                        "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat()
                    })

        # Condition hybride : Lot plein OU Temps écoulé (avec au moins 1 élément en attente)
        time_elapsed = time.time() - last_flush_time
        if len(batch_data) >= BATCH_SIZE or (len(batch_data) > 0 and time_elapsed >= FLUSH_INTERVAL_SEC):
            reason = "BATCH_SIZE atteint" if len(batch_data) >= BATCH_SIZE else "TIMEOUT écoulé"
            print(f"⏳ Déclenchement du flush ({reason} : {len(batch_data)} lignes)...")

            if flush_to_s3(batch_data, consumer):
                batch_data.clear()
                last_flush_time = time.time()


if __name__ == "__main__":
    main()