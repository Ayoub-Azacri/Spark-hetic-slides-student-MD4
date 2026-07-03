import os
import time
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialisation de la SparkSession
spark = SparkSession.builder \
    .appName("Group Explorations") \
    .master("local[*]") \
    .getOrCreate()

SILVER_DIR = "data/output/clean"
BENCHMARK_DIR = "data/output/benchmark_group"

# Nettoyage des anciens dossiers
if os.path.exists(BENCHMARK_DIR):
    shutil.rmtree(BENCHMARK_DIR)
os.makedirs(BENCHMARK_DIR)

# Chargement des tables Silver de base
df_caract = spark.read.parquet(f"{SILVER_DIR}/caracteristiques")
df_usagers = spark.read.parquet(f"{SILVER_DIR}/usagers")

print("=" * 60)
print("                  BENCHMARK DES EXPLORATIONS")
print("=" * 60)

# =========================================================================
# 1. EXPLORATION AYOUB AZACRI : Benchmark de formats (CSV, Parquet, JSON)
# =========================================================================
print("\n>>> 1. EXPLORATION AYOUB AZACRI : BENCHMARK DE FORMATS (CSV vs PARQUET vs JSON)")
write_times = {}
sizes = {}
read_times = {}

dfs_to_write = {
    "caracteristiques": df_caract,
    "usagers": df_usagers
}

# Écriture
for fmt in ["csv", "parquet", "json"]:
    start = time.time()
    for name, df in dfs_to_write.items():
        path = f"{BENCHMARK_DIR}/{fmt}/{name}"
        if fmt == "csv":
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
        elif fmt == "parquet":
            df.coalesce(1).write.mode("overwrite").parquet(path)
        elif fmt == "json":
            df.coalesce(1).write.mode("overwrite").json(path)
    write_times[fmt] = time.time() - start

# Mesure Taille
def get_dir_size(path):
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if not os.path.islink(fp):
                total += os.path.getsize(fp)
    return total / (1024 * 1024)

for fmt in ["csv", "parquet", "json"]:
    sizes[fmt] = get_dir_size(f"{BENCHMARK_DIR}/{fmt}")

# Lecture & Requête
for fmt in ["csv", "parquet", "json"]:
    start = time.time()
    if fmt == "csv":
        c = spark.read.option("header", "true").csv(f"{BENCHMARK_DIR}/{fmt}/caracteristiques")
        u = spark.read.option("header", "true").csv(f"{BENCHMARK_DIR}/{fmt}/usagers")
    elif fmt == "parquet":
        c = spark.read.parquet(f"{BENCHMARK_DIR}/{fmt}/caracteristiques")
        u = spark.read.parquet(f"{BENCHMARK_DIR}/{fmt}/usagers")
    elif fmt == "json":
        c = spark.read.json(f"{BENCHMARK_DIR}/{fmt}/caracteristiques")
        u = spark.read.json(f"{BENCHMARK_DIR}/{fmt}/usagers")

    res = c.join(u, "Num_Acc").groupBy("dep").count()
    res.count()
    read_times[fmt] = time.time() - start

print(f"[-] Ecriture  -> JSON: {write_times['json']:.2f}s | PARQUET: {write_times['parquet']:.2f}s | CSV: {write_times['csv']:.2f}s")
print(f"[-] Taille    -> JSON: {sizes['json']:.2f} Mo | PARQUET: {sizes['parquet']:.2f} Mo | CSV: {sizes['csv']:.2f} Mo")
print(f"[-] Lecture   -> JSON: {read_times['json']:.2f}s | PARQUET: {read_times['parquet']:.2f}s | CSV: {read_times['csv']:.2f}s")


# =========================================================================
# 2. EXPLORATION YOUSSEF EL HAJJI : Impact du Caching (Cache vs No-Cache)
# =========================================================================
# TODO: Youssef EL HAJJI code goes here


# =========================================================================
# 3. EXPLORATION OMAR HAKIK : Predicate Pushdown (Filtre au niveau stockage)
# =========================================================================
# TODO: Omar HAKIK code goes here


# =========================================================================
# 4. EXPLORATION AYOUB AZACRI : Repartition vs Coalesce
# =========================================================================
print("\n>>> 4. EXPLORATION AYOUB AZACRI : REPARTITION vs COALESCE")

df_large = spark.read.parquet(f"{SILVER_DIR}/caracteristiques")
print(f"[-] Nombre de partitions initiales : {df_large.rdd.getNumPartitions()}")

# Repartition (provoque un Shuffle complet)
start = time.time()
df_rep = df_large.repartition(4)
df_rep.count()
repartition_time = time.time() - start
print(f"[-] Temps avec .repartition(4) (avec Shuffle)  : {repartition_time:.3f}s")

# Coalesce (évite le Shuffle en fusionnant localement)
start = time.time()
df_coal = df_large.coalesce(4)
df_coal.count()
coalesce_time = time.time() - start
print(f"[-] Temps avec .coalesce(4) (sans Shuffle)    : {coalesce_time:.3f}s")


# Nettoyage des répertoires temporaires
if os.path.exists(BENCHMARK_DIR):
    shutil.rmtree(BENCHMARK_DIR)

spark.stop()
print("\n" + "=" * 60)
print("                 FIN DES MESURES")
print("=" * 60)
