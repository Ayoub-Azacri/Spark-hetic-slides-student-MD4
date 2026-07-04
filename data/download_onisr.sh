#!/usr/bin/env bash
# Telecharge les bases de donnees ONISR Accidents 2023.
set -euo pipefail

TARGET_DIR="data/datasets/onisr-2023"
mkdir -p "$TARGET_DIR"

echo "== Téléchargement des données ONISR 2023 =="

# 1. Caractéristiques
caract_url="https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/20241028-103125/caract-2023.csv"
echo "  caract-2023.csv..."
curl -fSL "$caract_url" -o "$TARGET_DIR/caract-2023.csv"

# 2. Lieux
lieux_url="https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/20241023-153219/lieux-2023.csv"
echo "  lieux-2023.csv..."
curl -fSL "$lieux_url" -o "$TARGET_DIR/lieux-2023.csv"

# 3. Véhicules
vehicules_url="https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/20241023-153253/vehicules-2023.csv"
echo "  vehicules-2023.csv..."
curl -fSL "$vehicules_url" -o "$TARGET_DIR/vehicules-2023.csv"

# 4. Usagers
usagers_url="https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/20241023-153328/usagers-2023.csv"
echo "  usagers-2023.csv..."
curl -fSL "$usagers_url" -o "$TARGET_DIR/usagers-2023.csv"

echo "Terminé. Fichiers téléchargés dans $TARGET_DIR :"
ls -lh "$TARGET_DIR"
