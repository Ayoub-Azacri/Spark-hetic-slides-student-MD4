import time
from spark_session import get_spark
import sys
import os

# Ajouter le dossier starter-code au chemin de recherche Python
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "starter-code"))
from pipeline import ingestion, nettoyage

def main():
    print("Démarrage de la SparkSession...")
    spark = get_spark("Projet Jour 4 - Keep UI Alive")
    
    print("\n=======================================================")
    print("Spark UI disponible sur : http://localhost:4040")
    print("=======================================================\n")
    
    # 1. Ingestion & Nettoyage (réutilise les chemins et schémas du pipeline réel)
    c_brut, l_brut, v_brut, u_brut = ingestion(spark)
    c_clean, l_clean, v_clean, u_clean = nettoyage(c_brut, l_brut, v_brut, u_brut)
    
    # 2. Caching
    from pyspark.sql import functions as F
    df_caract = c_clean.cache()
    df_usagers = u_clean.cache()
    
    # 3. Actions pour générer le DAG de jointure
    print("Exécution des jointures optimisées (Broadcast)...")
    
    df_caract_mapped = df_caract.withColumn("conditions_meteo", F.col("atm"))
    df_usagers_mapped = df_usagers.withColumn("gravite", F.col("grav"))
    
    res1 = df_caract_mapped.join(F.broadcast(df_usagers_mapped), "Num_Acc")
    res1.count() # Force l'action pour créer les jobs dans la Spark UI
    
    print("\nLe pipeline est chargé et maintenu actif en mémoire.")
    print("Vous pouvez maintenant ouvrir http://localhost:4040 dans votre navigateur.")
    print("Prenez vos captures d'écran des Jobs, Stages, et du DAG de la jointure.")
    print("\nAppuyez sur Ctrl+C pour arrêter le script et fermer la Spark UI.")
    
    try:
        # Reste actif pendant 15 minutes
        time.sleep(900)
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur...")
    finally:
        spark.stop()
        print("SparkSession fermée avec succès.")

if __name__ == "__main__":
    main()
