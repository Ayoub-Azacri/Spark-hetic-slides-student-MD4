"""Squelette de pipeline data pour le projet du jour 4.

Complétez les sections marquées TODO avec le jeu de données que vous avez choisi
(taxi NYC multi-mois, DVF immobilier, accidents ONISR, ou MovieLens).

Architecture cible (vue en cours) :
    brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Lancement, depuis la racine du projet :
    python starter-code/pipeline.py

L'énoncé complet et la grille : projects/projet-jour-4.md
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import get_spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType

# Chemins des données ONISR 2023
DATA_DIR = "data/datasets/onisr-2023"
CARACT_BRUT = f"{DATA_DIR}/caract-2023.csv"
LIEUX_BRUT = f"{DATA_DIR}/lieux-2023.csv"
VEHICULES_BRUT = f"{DATA_DIR}/vehicules-2023.csv"
USAGERS_BRUT = f"{DATA_DIR}/usagers-2023.csv"

# Dossiers de sortie
SORTIE_SILVER = "data/output/clean"
SORTIE_GOLD = "data/output/analyses"



def ingestion(spark):
    """Étape 1a : lire les données brutes avec un schéma explicite."""
    print("--- INGESTION DES DONNÉES BRUTES ---")

    # Schéma Caractéristiques
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

    # Schéma Lieux
    lieux_schema = StructType([
        StructField("Num_Acc", LongType(), False),
        StructField("catr", IntegerType(), True),
        StructField("voie", StringType(), True),
        StructField("v1", IntegerType(), True),
        StructField("v2", StringType(), True),
        StructField("circ", IntegerType(), True),
        StructField("nbv", IntegerType(), True),
        StructField("vosp", IntegerType(), True),
        StructField("prof", IntegerType(), True),
        StructField("pr", StringType(), True),
        StructField("pr1", StringType(), True),
        StructField("plan", IntegerType(), True),
        StructField("lartpc", StringType(), True),
        StructField("larrout", StringType(), True),
        StructField("surf", IntegerType(), True),
        StructField("infra", IntegerType(), True),
        StructField("situ", IntegerType(), True),
        StructField("vma", IntegerType(), True)
    ])

    # Schéma Véhicules
    vehicules_schema = StructType([
        StructField("Num_Acc", LongType(), False),
        StructField("id_vehicule", StringType(), True),
        StructField("num_veh", StringType(), True),
        StructField("senc", IntegerType(), True),
        StructField("catv", IntegerType(), True),
        StructField("obs", IntegerType(), True),
        StructField("obsm", IntegerType(), True),
        StructField("choc", IntegerType(), True),
        StructField("manv", IntegerType(), True),
        StructField("motor", IntegerType(), True),
        StructField("occutc", IntegerType(), True)
    ])

    # Schéma Usagers
    usagers_schema = StructType([
        StructField("Num_Acc", LongType(), False),
        StructField("id_usager", StringType(), True),
        StructField("id_vehicule", StringType(), True),
        StructField("num_veh", StringType(), True),
        StructField("place", IntegerType(), True),
        StructField("catu", IntegerType(), True),
        StructField("grav", IntegerType(), True),
        StructField("sexe", IntegerType(), True),
        StructField("an_nais", IntegerType(), True),
        StructField("trajet", IntegerType(), True),
        StructField("secu1", IntegerType(), True),
        StructField("secu2", IntegerType(), True),
        StructField("secu3", IntegerType(), True),
        StructField("locp", IntegerType(), True),
        StructField("actp", IntegerType(), True),
        StructField("etatp", IntegerType(), True)
    ])

    # Lecture des CSV
    df_caract = spark.read.option("header", "true").option("sep", ";").schema(caract_schema).csv(CARACT_BRUT)
    df_lieux = spark.read.option("header", "true").option("sep", ";").schema(lieux_schema).csv(LIEUX_BRUT)
    df_vehicules = spark.read.option("header", "true").option("sep", ";").schema(vehicules_schema).csv(VEHICULES_BRUT)
    df_usagers = spark.read.option("header", "true").option("sep", ";").schema(usagers_schema).csv(USAGERS_BRUT)

    print(f"Lignes lues - caractéristiques: {df_caract.count()}")
    print(f"Lignes lues - lieux: {df_lieux.count()}")
    print(f"Lignes lues - véhicules: {df_vehicules.count()}")
    print(f"Lignes lues - usagers: {df_usagers.count()}")

    return df_caract, df_lieux, df_vehicules, df_usagers



def nettoyage(df_caract, df_lieux, df_vehicules, df_usagers):
    """Étape 1b : typer, dériver des colonnes, nettoyer (bronze -> silver)."""
    print("\n--- NETTOYAGE & VALIDATION ---")
    # 1. Nettoyage de Caractéristiques
    df_caract_clean = df_caract.dropDuplicates(["Num_Acc"])
    df_caract_clean = df_caract_clean.filter(F.col("Num_Acc").isNotNull())
    # Remplacer la virgule par un point pour latitude/longitude, puis caster en Double
    df_caract_clean = df_caract_clean.withColumn("lat_clean", F.regexp_replace(F.col("lat"), ",", ".").cast(DoubleType())).withColumn("long_clean", F.regexp_replace(F.col("long"), ",", ".")
                                    .cast(DoubleType()))
    # Création d'un timestamp propre (format : yyyy M d HH:mm pour tolérer les mois à un seul chiffre)
    df_caract_clean = df_caract_clean.withColumn(
            "date_accident",
            F.to_timestamp(
            F.concat_ws(" ", F.col("an"),
            F.col("mois"), F.col("jour"),
            F.col("hrmn")),
            "yyyy M d HH:mm"
            )
    )

    # 2. Nettoyage de Lieux
    df_lieux_clean = df_lieux.dropDuplicates(["Num_Acc"])
    df_lieux_clean = df_lieux_clean.filter(F.col("Num_Acc").isNotNull())

    # 3. Nettoyage de Véhicules
    # Supprimer les espaces insécables dans id_vehicule
    df_vehicules_clean = df_vehicules.withColumn(
            "id_vehicule_clean",
            F.regexp_replace(F.col("id_vehicule"), r"\s+","")
    )
    df_vehicules_clean = df_vehicules_clean.dropDuplicates(["Num_Acc","id_vehicule_clean"])
    df_vehicules_clean = df_vehicules_clean.filter(
            F.col("Num_Acc").isNotNull() & F.col("id_vehicule_clean").isNotNull()
    )

    # 4. Nettoyage de Usagers
    df_usagers_clean = df_usagers.withColumn(
            "id_usager_clean",
            F.regexp_replace(F.col("id_usager"), r"\s+", "")
    ).withColumn(
            "id_vehicule_clean",
            F.regexp_replace(F.col("id_vehicule"), r"\s+","")
        )
    df_usagers_clean = df_usagers_clean.dropDuplicates(["id_usager_clean"])

    # Filtrer les années de naissance réalistes et gravités valides (1=indemne à 4=blessé léger)
    df_usagers_clean = df_usagers_clean.filter(
            F.col("Num_Acc").isNotNull() &
            (F.col("grav") >= 1) & (F.col("grav") <= 4) &
            ((F.col("an_nais") >= 1900) & (F.col("an_nais") <= 2023) | F.col("an_nais").isNull())
    )
    # -------------------------------------------------------------------------
        # CALCUL ET AFFICHAGE DES STATISTIQUES DE QUALITÉ DES DONNÉES
    # -------------------------------------------------------------------------
    count_caract_brut = df_caract.count()
    count_caract_clean = df_caract_clean.count()
    drop_caract = ((count_caract_brut - count_caract_clean) / count_caract_brut) * 100

    count_lieux_brut = df_lieux.count()
    count_lieux_clean = df_lieux_clean.count()
    drop_lieux = ((count_lieux_brut - count_lieux_clean) / count_lieux_brut) * 100

    count_veh_brut = df_vehicules.count()
    count_veh_clean = df_vehicules_clean.count()
    drop_veh = ((count_veh_brut - count_veh_clean) / count_veh_brut) * 100

    count_usagers_brut = df_usagers.count()
    count_usagers_clean = df_usagers_clean.count()
    drop_usagers = ((count_usagers_brut - count_usagers_clean) / count_usagers_brut) * 100

    print(f"Après nettoyage - caractéristiques: {count_caract_clean} ({drop_caract:.2f}% écartées)")
    print(f"Après nettoyage - lieux: {count_lieux_clean} ({drop_lieux:.2f}% écartées)")
    print(f"Après nettoyage - véhicules: {count_veh_clean} ({drop_veh:.2f}% écartées)")
    print(f"Après nettoyage - usagers: {count_usagers_clean} ({drop_usagers:.2f}% écartées)")
 # -------------------------------------------------------------------------

    return df_caract_clean, df_lieux_clean, df_vehicules_clean, df_usagers_clean



def ecrire_silver(df_caract, df_lieux, df_vehicules, df_usagers):
    """Étape 1c : écrire la couche intermédiaire nettoyée en Parquet."""
    print("\n--- ÉCRITURE COUCHE SILVER (PARQUET) ---")

    # On partitionne les caractéristiques par département (dep)
    df_caract.write.mode("overwrite").partitionBy("dep").parquet(f"{SORTIE_SILVER}/caracteristiques")
    df_lieux.write.mode("overwrite").parquet(f"{SORTIE_SILVER}/lieux")
    df_vehicules.write.mode("overwrite").parquet(f"{SORTIE_SILVER}/vehicules")
    df_usagers.write.mode("overwrite").parquet(f"{SORTIE_SILVER}/usagers")

    print(f"Couche silver écrite avec succès dans : {SORTIE_SILVER}")


def transformation_et_analyses(spark):
    """Étape 2 : relire le propre, puis 3 analyses (silver -> gold)."""
    print("\n--- TRANSFORMATION & ANALYSES (SILVER -> GOLD) ---")
    
    # Relire chaque table depuis la couche silver
    df_caract = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")
    df_lieux = spark.read.parquet(f"{SORTIE_SILVER}/lieux")
    df_vehicules = spark.read.parquet(f"{SORTIE_SILVER}/vehicules")
    df_usagers = spark.read.parquet(f"{SORTIE_SILVER}/usagers")

    # Cacher les DataFrames réutilisés pour optimiser la Spark UI
    df_caract = df_caract.cache()
    df_usagers = df_usagers.cache()

    # --- Analyse 1 : agrégation -------------------------------
    # Question : Nombre de victimes par gravité selon les conditions atmosphériques
    df_caract_mapped = df_caract.withColumn(
        "conditions_meteo",
        F.when(F.col("atm") == 1, "Normale")
        .when(F.col("atm") == 2, "Pluie légère")
        .when(F.col("atm") == 3, "Pluie forte")
        .when(F.col("atm") == 4, "Neige / grêle")
        .when(F.col("atm") == 5, "Brouillard / fumée")
        .when(F.col("atm") == 6, "Vent fort / tempête")
        .when(F.col("atm") == 7, "Temps éblouissant")
        .when(F.col("atm") == 8, "Temps nuageux")
        .otherwise("Autre / Non renseigné")
    )
    
    df_usagers_mapped = df_usagers.withColumn(
        "gravite",
        F.when(F.col("grav") == 1, "Indemne")
        .when(F.col("grav") == 2, "Tué")
        .when(F.col("grav") == 3, "Blessé hospitalisé")
        .when(F.col("grav") == 4, "Blessé léger")
        .otherwise("Inconnu")
    )

    analyse_1 = df_caract_mapped.join(df_usagers_mapped, "Num_Acc") \
        .groupBy("conditions_meteo", "gravite") \
        .count() \
        .orderBy("conditions_meteo", "gravite")

    # --- Analyse 2 : jointure ---------------------------------
    # Question : Taux de gravité des blessures par catégorie de véhicule
    df_veh_mapped = df_vehicules.withColumn(
        "categorie_vehicule",
        F.when(F.col("catv") == 7, "Voiture (VL)")
        .when(F.col("catv") == 10, "Utilitaire (VU)")
        .when(F.col("catv") == 33, "Poids lourd (PL)")
        .when(F.col("catv") == 1, "Vélo")
        .when(F.col("catv") == 2, "Cyclomoteur")
        .when(F.col("catv").isin(30, 31, 32, 33, 34, 35, 36, 40), "Moto / Scooter")
        .otherwise("Autre")
    )

    analyse_2 = df_usagers.join(df_veh_mapped, ["Num_Acc", "id_vehicule_clean"]) \
        .groupBy("categorie_vehicule") \
        .agg(
            F.count("id_usager_clean").alias("total_usagers"),
            F.sum(F.when(F.col("grav").isin(2, 3), 1).otherwise(0)).alias("total_graves"),
            F.round((F.sum(F.when(F.col("grav").isin(2, 3), 1).otherwise(0)) / F.count("id_usager_clean")) * 100, 2).alias("taux_gravite_pct")
        ) \
        .orderBy(F.desc("taux_gravite_pct"))

    # --- Analyse 3 : window function  ------------------------------
    # Question : Top 3 des départements les plus accidentogènes pour chaque mois
    from pyspark.sql.window import Window
    
    # Agrégation par mois et département
    df_dep_monthly = df_caract.filter(F.col("dep").isNotNull() & F.col("mois").isNotNull()) \
        .groupBy("mois", "dep") \
        .agg(F.count_distinct("Num_Acc").alias("total_accidents"))

    
    # Définition de la fenêtre ordonnée par nombre d'accidents décroissant
    window_spec = Window.partitionBy("mois").orderBy(F.desc("total_accidents"))
    
    # Classement
    analyse_3 = df_dep_monthly.withColumn("rang", F.dense_rank().over(window_spec)) \
        .filter(F.col("rang") <= 3) \
        .orderBy("mois", "rang")

    return {"gravite_meteo": analyse_1, "gravite_vehicule": analyse_2, "top_dep_par_mois": analyse_3}


def ecrire_gold(resultats):
    """Étape 3 : écrire les résultats de synthèse en CSV."""
    print("\n--- ÉCRITURE COUCHE GOLD (CSV) ---")
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(chemin)
        print("Résultat écrit (CSV) :", chemin)


def main():
    spark = get_spark("Projet Jour 4 - Mon pipeline")
    print("Spark UI disponible sur http://localhost:4040")

    # Étape 1 : ingestion et nettoyage (bronze -> silver)
    df_caract_brut, df_lieux_brut, df_vehicules_brut, df_usagers_brut = ingestion(spark)

    df_caract_clean, df_lieux_clean, df_vehicules_clean, df_usagers_clean = nettoyage(
        df_caract_brut, df_lieux_brut, df_vehicules_brut, df_usagers_brut
    )

    ecrire_silver(df_caract_clean, df_lieux_clean, df_vehicules_clean, df_usagers_clean)

    # Étape 2 : transformation et analyses (silver -> gold)
    resultats = transformation_et_analyses(spark)

    # Étape 3 : finalisation
    ecrire_gold(resultats)

    print("\n--- PIPELINE COMPLET DE BOUT EN BOUT RÉUSSI ! ---")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans starter-code/pipeline.py.")
        sys.exit(1)
