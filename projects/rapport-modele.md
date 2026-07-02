# Rapport de projet - Pipeline Spark (Jour 4)

Gabarit du livrable noté. Remplir chaque section. Court et dense : extraits de code, extraits de
résultats, captures. Pas de pavé. Les sections reprennent le plan du rapport (section 5 de
`projects/projet-jour-4.md`). La grille reste le barème ; la qualité du code est notée sur le code
lui-même, pas dans ce document.

- **Équipe** : [Ayoub Azacri & Omar Hakik & Youssef El HAJJI , Youssef DEKHAIL]
- **Jeu de données** : ONISR (Accidents corporels 2023)
- **Date** : 26 Juin 2026

---

## 1. Jeu de données et schéma cible

- **Source et volume** : Fichiers BAAC de l'ONISR (data.gouv.fr). Volume total de ~33 Mo (4 tables CSV relationnelles : Caractéristiques, Lieux, Véhicules, Usagers).
- **Schéma cible** :
  * caractéristiques : `Num_Acc` (Long), `dep` (String), `date_accident` (Timestamp), `lat_clean` (Double), `long_clean` (Double).
  * lieux : `Num_Acc` (Long), `catr` (Int), `vma` (Int), `surf` (Int).
  * véhicules : `Num_Acc` (Long), `id_vehicule_clean` (String), `catv` (Int).
  * usagers : `Num_Acc` (Long), `id_usager_clean` (String), `grav` (Int), `an_nais` (Int).
- **Questions métier visées** :
  1. Répartition de la gravité par conditions météo.
  2. Gravité moyenne des accidents par catégorie de véhicule.
  3. Départements les plus accidentogènes par mois.

---

## 2. Pipeline (bronze -> silver -> gold)

```
brut (bronze)  ->  nettoyage (silver, Parquet)  ->  agrégé (gold)
```

- **Nettoyage appliqué** : Suppression des doublons (sur `Num_Acc`), nettoyage des identifiants (suppression des espaces insécables), casting des coordonnées GPS (remplacement de `,` par `.`), création d'un timestamp standardisé (`date_accident`), et filtrage des valeurs aberrantes (âges réalistes, gravité valide).
- **Chiffres des lignes** :
  * caractéristiques : 54 822 brutes | 54 822 propres | 0 écartées (0.00%)
  * lieux : 70 860 brutes | 54 822 propres | 16 038 écartées (22.63%)
  * véhicules : 93 585 brutes | 93 585 propres | 0 écartées (0.00%)
  * usagers : 125 789 brutes | 125 671 propres | 118 écartées (0.09%)
- **Partitionnement de la silver** : Table `caracteristiques` partitionnée par **`dep` (département)** pour optimiser les filtres géographiques (Partition Pruning).

---

## 3. Analyses
### Analyse 1 - agrégation 

- Question : Quelle est la répartition de la gravité des blessures des victimes selon les conditions atmosphériques ?
- Code clé :
```python
analyse_1 = df_caract_mapped.join(df_usagers_mapped, "Num_Acc") \
    .groupBy("conditions_meteo", "gravite") \
    .count() \
    .orderBy("conditions_meteo", "gravite")
```
- Résultat (extrait) :
```
conditions_meteo,gravite,count
Normale,Indemne,41931
Normale,Blessé léger,38611
Normale,Blessé hospitalisé,15197
Normale,Tué,2633
Pluie légère,Indemne,6293
Pluie légère,Blessé léger,6350
Pluie légère,Blessé hospitalisé,1967
Pluie légère,Tué,345
Pluie forte,Indemne,1446
Pluie forte,Blessé léger,1353
Pluie forte,Blessé hospitalisé,536
Pluie forte,Tué,100
```
- Lecture métier : Contre-intuitivement, l'immense majorité des accidents corporels (et des décès, soit plus de 70%) a lieu par temps météo "Normal". Cela s'explique par le fait que les conducteurs sont moins vigilants et roulent plus vite lorsque les conditions sont optimales. La pluie légère représente le second facteur météo le plus accidentogène avec 345 tués.

### Analyse 2 - jointure 

- Question : Quel est le taux de gravité (pourcentage d'accidents mortels et hospitalisés) selon la catégorie du véhicule impliqué ?
- Code clé :
```python
analyse_2 = df_usagers.join(df_veh_mapped, ["Num_Acc", "id_vehicule_clean"]) \
    .groupBy("categorie_vehicule") \
    .agg(
        F.count("id_usager_clean").alias("total_usagers"),
        F.sum(F.when(F.col("grav").isin(2, 3), 1).otherwise(0)).alias("total_graves"),
        F.round((F.sum(F.when(F.col("grav").isin(2, 3), 1).otherwise(0)) / F.count("id_usager_clean")) * 100, 2).alias("taux_gravite_pct")
    ) \
    .orderBy(F.desc("taux_gravite_pct"))
```
- Résultat (extrait) :
```
categorie_vehicule,total_usagers,total_graves,taux_gravite_pct
Poids lourd (PL),8709,3649,41.9
Cyclomoteur,3833,1446,37.73
Vélo,5459,1442,26.42
Moto / Scooter,8330,2177,26.13
Voiture (VL),78741,11270,14.31
Utilitaire (VU),8975,990,11.03
```
- Lecture métier : Les accidents impliquant des Poids Lourds (PL) présentent le taux de gravité le plus élevé (41.90% de tués ou hospitalisés), en raison de leur masse et de l'énergie cinétique du choc. De même, les usagers de deux-roues (Cyclomoteurs à 37.73%, Vélos à 26.42% et Motos à 26.13%) subissent des blessures très graves à cause de leur absence de carrosserie protectrice, contrairement aux Voitures (VL) à 14.31%.


### Analyse 3 - window function 

- Question : Quels sont les 3 départements les plus accidentogènes de France pour chaque mois de l'année ?
- Code clé :
```python
window_spec = Window.partitionBy("mois").orderBy(F.desc("total_accidents"))

analyse_3 = df_dep_monthly.withColumn("rang", F.dense_rank().over(window_spec)) \
    .filter(F.col("rang") <= 3) \
    .orderBy("mois", "rang")
```
- Résultat (extrait) :
```
+----+---+---------------+----+
|mois|dep|total_accidents|rang|
+----+---+---------------+----+
|   1| 75|            353|   1|
|   1| 92|            201|   2|
|   1| 93|            194|   3|
|   2| 75|            318|   1|
|   2| 92|            189|   2|
|   2| 93|            186|   3|
|   3| 75|            384|   1|
|   3| 92|            216|   2|
|   3| 93|            208|   3|
+----+---+---------------+----+
```
- Lecture métier : Paris (département 75) est systématiquement le département le plus accidentogène de France pour chaque mois de l'année (ex: 353 accidents en Janvier, 428 en Mai). Il est systématiquement suivi par les Hauts-de-Seine (92) et la Seine-Saint-Denis (93) en deuxième et troisième positions, démontrant une forte concentration des accidents corporels dans l'agglomération parisienne à haute densité de trafic.

---

## 4. Optimisation

- Optimisation choisie : Caching des tables principales réutilisées + Jointure par diffusion (`F.broadcast`).
- Pourquoi : 
  1. Les DataFrames `df_caract` et `df_usagers` sont lus depuis le stockage et réutilisés dans plusieurs analyses consécutives. Les mettre en cache (`.cache()`) évite de relire les fichiers Parquet sur disque à chaque action Spark.
  2. Les tables `usagers` (~125k lignes) et `véhicules` (~93k lignes) font moins de 10 Mo en mémoire. Diffuser ces tables via `F.broadcast()` permet à Spark d'utiliser un `BroadcastHashJoin`. Cela élimine l'étape de Shuffle (distribution réseau des données par clé) requise par un `SortMergeJoin` classique.
- Mesure avant/après ou extrait de plan :
```
Sans optimisation (Explain physical plan) :
  AdaptiveSparkPlan uses BroadcastHashJoin (AQE auto-detects small sizes at runtime).

Avec optimisation explicite (Explain physical plan) :
  +- BroadcastHashJoin [Num_Acc#0L], [Num_Acc#18L], Inner, BuildRight, false
     :- Filter isnotnull(Num_Acc#0L)
     +- BroadcastExchange HashedRelationBroadcastMode(...)
```
- Ce que ça change : Temps d'exécution de la phase d'analyses divisé par 2 (environ 1.8s gagnées) et absence totale de Shuffle Write/Shuffle Read réseau sur ces jointures, garantissant une meilleure scalabilité sur des clusters distribués.

---

## 5. Lecture de la Spark UI

*Note sur la démarche : Le pipeline principal `pipeline.py` est conçu pour s'exécuter de manière 100% automatisée (sans blocage). Afin d'analyser la Spark UI localement sans polluer le code de production, nous avons développé un utilitaire dédié `keep_ui_alive.py` pour maintenir la session active le temps d'observer les plans d'exécution.*

- Job observé : Job 31 (Transformation et écriture Gold)
- Où se produit le shuffle (`Exchange`) : Le shuffle se produit lors de l'action finale `coalesce(1)` et de l'agrégation `groupBy` avant l'écriture des fichiers CSV.
- Nombre de stages et de tasks : 
  * Nombre de stages : 3 stages distincts.
  * Nombre de tasks : 64 tasks exécutées en parallèle au maximum pour les lectures et jointures de départ (comme visible sur les Jobs 29/31/33), puis réduites à 1 task pour la phase d'écriture Gold coalescée (Job 34).
- Capture(s) :
  * **DAG de la jointure** : ![Spark UI DAG](screenshots/spark_ui_dag.svg)
  * **Timeline des tâches** : ![Spark UI Timeline](screenshots/spark_ui_timeline.png)
  * **Plan SQL logique** : ![Spark UI SQL](screenshots/spark_ui_sql.svg)
  *(Toutes les captures sont visualisables dans le dossier local [projects/screenshots](file:///home/ayoubazacri/Desktop/HETIC/bigData-spark/Spark-hetic-slides-student/projects/screenshots/))*
- Commentaire : L'absence de shuffle sur les jointures confirme l'efficacité du broadcast join. Le goulot d'étranglement restant est le `coalesce(1)` imposé par le sujet pour générer un fichier CSV unique, qui force la centralisation de toutes les données sur le driver Spark.

---

## 6. Exploration au-delà du cours

- Piste choisie : [AQE et partitions / skew et salting / UDF vs pandas_udf / table gérée et upsert /
  spark-submit / pushdown mesuré / benchmark formats / streaming ou MLlib]
- Question : [...]
- Protocole (ce qu'on a fait varier, ce qui reste fixe) : [...]
- Mesures :
```
[...]
```
- Conclusion (même si négative ou contre-intuitive) : [...]

---

## 7. Ce qu'on a appris et limites

- Ce qui a marché : [...]
- Ce qui a bloqué : [...]
- Ce qu'on ferait avec plus de temps : [...]
