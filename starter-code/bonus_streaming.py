import os
import shutil
import time
import threading
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
import pyspark.sql.functions as F

# Configuration dynamique des répertoires de streaming
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

STREAMING_DIR = os.path.join(PROJECT_ROOT, "data", "streaming_input")
CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "streaming_checkpoint")
RAW_CARACT_PATH = os.path.join(PROJECT_ROOT, "data", "datasets", "onisr-2023", "caract-2023.csv")

def init_streaming_directories():
    """Réinitialise les dossiers temporaires de streaming."""
    print("[-] Réinitialisation des répertoires de streaming...")
    for path in [STREAMING_DIR, CHECKPOINT_DIR]:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

def stream_simulator_thread():
    """Simule l'arrivée continue de fichiers de données toutes les 6 secondes."""
    time.sleep(3)  # Attendre le démarrage de Spark
    print("\n[SIMULATEUR] Lancement de la simulation de flux en temps réel...")
    
    with open(RAW_CARACT_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    header = lines[0]
    data_lines = lines[1:]
    chunk_size = 15000  # 15 000 lignes par micro-batch
    
    chunks = [data_lines[i:i + chunk_size] for i in range(0, len(data_lines), chunk_size)]
    
    for idx, chunk in enumerate(chunks):
        chunk_file = os.path.join(STREAMING_DIR, f"accidents_batch_{idx + 1}.csv")
        print(f"\n[SIMULATEUR] --> Dépôt du batch {idx + 1} ({len(chunk)} lignes)...")
        with open(chunk_file, "w", encoding="utf-8") as out:
            out.write(header)
            out.writelines(chunk)
        time.sleep(6)
        
    print("\n[SIMULATEUR] Simulation terminée !")
    print("[INFO] Appuyez sur Ctrl+C pour arrêter le streaming.")

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

    # 5. Lancement du thread simulateur
    simulator = threading.Thread(target=stream_simulator_thread, daemon=True)
    simulator.start()
    
    # 6. Écriture du flux vers la console (mode complet)
    query = df_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()
        
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[-] Arrêt demandé.")
    finally:
        query.stop()
        spark.stop()

if __name__ == "__main__":
    run_streaming()
