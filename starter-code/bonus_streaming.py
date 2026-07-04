import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

# Configuration des répertoires de streaming
STREAMING_DIR = "data/streaming_input"
CHECKPOINT_DIR = "data/streaming_checkpoint"

def init_streaming_directories():
    """Réinitialise les dossiers temporaires de streaming."""
    print("[-] Réinitialisation des répertoires de streaming...")
    for path in [STREAMING_DIR, CHECKPOINT_DIR]:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

def run_streaming():
    # Initialisation des répertoires
    init_streaming_directories()

    # 1. Session Spark 
    spark = SparkSession.builder \
        .appName("ONISR-Accidents-Streaming-Bonus") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n============================================================")
    print("         BONUS : PIPELINE STRUCTURED STREAMING (ONISR)")
    print("============================================================\n")

    # 2. Définition du schéma caractéristiques 
    caract_schema = StructType([
        StructField("Num_Acc", LongType(), False),
        StructField("jour", IntegerType(), True),
        StructField("mois", IntegerType(), True),
        StructField("an", IntegerType(), True),
        StructField("hrmn", StringType(), True),
        StructField("lum", IntegerType(), True),
        StructField("dep", StringType(), True),
        StructField("com", StringType(), True),
        StructField("agg", IntegerType(), True),
        StructField("int", IntegerType(), True),
        StructField("atm", IntegerType(), True),
        StructField("col", IntegerType(), True),
        StructField("adr", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("long", StringType(), True)
    ])

    # 3. Lecture du flux de données CSV 
    print("[-] En attente de données dans : " + STREAMING_DIR)
    df_stream = spark.readStream \
        .option("header", "true") \
        .option("sep", ";") \
        .schema(caract_schema) \
        .csv(STREAMING_DIR)

    # 4. Traitement en streaming : Nombre d'accidents cumulé par département 
    import pyspark.sql.functions as F
    df_counts = df_stream.groupBy("dep") \
        .count() \
        .orderBy(F.desc("count"))

    # TODO (Youssef EL HAJJI) : ÉTAPE 3 - Lancement du Stream & Simulateur de flux
    # - Déposer des fichiers CSV bruts de test dans data/streaming_input/
    # - Écrire le flux vers la console (mode "complete") avec writeStream
    # - Démarrer le stream

if __name__ == "__main__":
    run_streaming()
