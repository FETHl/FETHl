from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import time

# Initialisation de la session Spark avec la configuration qui fonctionnait
# Notez que l'URI est "mongo:27017" et non "mongodb:27018"
spark = SparkSession.builder \
    .appName("StructuredStreamingKafkaMongoDB") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/logs.status_counts") \
    .getOrCreate()

print("🔌 Connexion à MongoDB: mongodb://mongo:27017/logs.status_counts")

# Lecture des logs depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "logs") \
    .load()

# Conversion des données en string
logs_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Extraction des champs en utilisant split() - Gardons la méthode qui fonctionnait
parsed_logs = logs_df.withColumn("log_parts", split(col("value"), " ")).select(
    col("log_parts")[0].alias("ip"),  # Adresse IP
    regexp_extract(col("value"), r'\[(.*?)\]', 1).alias("timestamp"),  # Extraire la date entre []
    regexp_extract(col("value"), r'"(\w+) ', 1).alias("method"),  # Verbe HTTP (GET, POST, etc.)
    regexp_extract(col("value"), r'"(?:\w+) (.*?) HTTP', 1).alias("url"),  # URL demandée
    regexp_extract(col("value"), r'HTTP/\d\.\d', 0).alias("protocol"),  # Protocole HTTP
    col("log_parts")[8].cast("int").alias("status"),  # Code HTTP
    when(size(col("log_parts")) > 9, col("log_parts")[9].cast("int")).alias("size")  # Taille de la réponse
)

# Correction du format du timestamp
parsed_logs = parsed_logs.withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z"))

# Détection des erreurs en temps réel (404 et 500 sur 5 minutes)
error_logs = parsed_logs.filter((col("status") == 404) | (col("status") == 500))
error_trends = error_logs \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "status") \
    .count()

# Produits en tendance (consultés plus de 20 fois en 1 minute)
popular_products = parsed_logs \
    .withColumn("product_id", regexp_extract(col("url"), r'id=(\d+)', 1)) \
    .filter(col("product_id") != "") \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute"), "product_id") \
    .count() \
    .filter(col("count") > 20)

# Surveillance de l'activité utilisateur (détection d'un volume anormal de requêtes)
suspicious_ips = parsed_logs \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute"), "ip") \
    .count() \
    .filter(col("count") > 100)

# Agrégation des logs par code HTTP
status_counts = parsed_logs.groupBy("status").count()

# Fonction pour écrire dans MongoDB avec résilience
def write_to_mongo(df, epoch_id, collection="status_counts"):
    """Écrit le DataFrame dans MongoDB avec gestion minimale des erreurs"""
    try:
        if not df.isEmpty():
            df.write.format("mongo") \
                .mode("append") \
                .option("collection", collection) \
                .option("replaceDocument", "false") \
                .save()
            print(f"✅ Données écrites avec succès dans la collection '{collection}' pour l'epoch {epoch_id}")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture dans MongoDB (collection: {collection}): {str(e)}")

# Configuration des points de contrôle pour la reprise
checkpoint_base = "/tmp/checkpoints/"
os.makedirs(checkpoint_base, exist_ok=True)

print("🚀 Démarrage des requêtes streaming...")

# Écriture des résultats dans MongoDB en streaming
query_status = status_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_mongo(df, eid, "status_counts")) \
    .option("checkpointLocation", checkpoint_base + "status") \
    .start()

query_errors = error_trends.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_mongo(df, eid, "error_trends")) \
    .option("checkpointLocation", checkpoint_base + "errors") \
    .start()

query_popular = popular_products.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_mongo(df, eid, "popular_products")) \
    .option("checkpointLocation", checkpoint_base + "popular") \
    .start()

query_suspicious = suspicious_ips.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_mongo(df, eid, "suspicious_ips")) \
    .option("checkpointLocation", checkpoint_base + "suspicious") \
    .start()

print("📊 Toutes les requêtes streaming ont démarré. En attente de terminaison...")

# Attente de la terminaison des streams avec gestion d'interruption
try:
    query_status.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️ Interruption détectée, arrêt propre des requêtes streaming...")
    query_status.stop()
    query_errors.stop()
    query_popular.stop()
    query_suspicious.stop()
    print("✅ Toutes les requêtes ont été arrêtées correctement.")