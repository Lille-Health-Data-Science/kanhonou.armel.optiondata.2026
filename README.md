# Application de suivi du diabète (FHIR) · M2 Data Science en santé, Lille

Projet « application diabète » de la séance 1 (FHIR) : suivi d'**un patient** à partir de recherches FHIR R4 sur le serveur public HAPI (`https://hapi.fhir.org/baseR4`). Données **fictives** uniquement.

## Contenu du dépôt

| Fichier | Rôle |
|---|---|
| `projet_diabete.ipynb` | L'application : crée un patient de démonstration fictif, puis récupère ses glycémies, son traitement en cours et trace la courbe (unités harmonisées en mg/dL) |
| `courbe_glycemie.png` | Courbe produite par le notebook |
| `REPONSES.md` | Réponses aux trois questions (ressources et recherches FHIR, authentification, intérêt de FHIR) |
| `seance1_fhir/` | Notebook du TP de la séance 1 complété (`seance1_etudiant.ipynb`), avec `LISEZMOI.md` et `requirements.txt` du kit (sans les données) |

## Lancer le projet

1. Python 3.10 ou plus, avec les paquets du kit :
   ```bash
   pip install -r seance1_fhir/requirements.txt
   ```
2. Ouvrir `projet_diabete.ipynb` **depuis la racine du dépôt**, puis *Restart & Run All*.

Un accès Internet est nécessaire. Chaque exécution crée un **nouveau** patient fictif (nom « M2DSS-Kanhonou », identifiant unique) sur le serveur public et régénère `courbe_glycemie.png`. Si le serveur est injoignable, le notebook l'indique clairement et les étapes suivantes sont ignorées sans erreur.

Le notebook du TP (`seance1_fhir/seance1_etudiant.ipynb`) a besoin du dossier `donnees/` du kit, non versionné : copiez-le dans `seance1_fhir/` pour le réexécuter.
