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

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

### Analyse 2 - jointure

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

### Analyse 3 - window function

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

---

## 4. Optimisation

- Optimisation choisie : [broadcast / cache / repartition]
- Pourquoi : [...]
- Mesure avant/après ou extrait de plan :
```
avant : [...] s   |   après : [...] s
(ou extrait de explain() montrant le changement)
```
- Ce que ça change : [...]

---

## 5. Lecture de la Spark UI

- Job observé : [...]
- Où se produit le shuffle (`Exchange`) : [...]
- Nombre de stages et de tasks : [...]
- Capture(s) : [insérer]
- Commentaire : [...]

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
