# Séance 2 · TD Airflow — Réponses

Armel Kanhonou · M2 Data Science en santé, Université de Lille

Installation : voie A (Docker Desktop, Windows). Le DAG `verification_installation` est passé au vert (3 tâches).

Fichiers de ce dossier :

| Fichier | Partie |
|---|---|
| `mon_premier_dag.py` | 01 · Premier DAG, avec la tâche `controler` ajoutée |
| `prevoir_executions.py` | 02 · Prévoir les exécutions (version `prevoir_executions_V2`, `catchup=False`) |
| `fhir_glycemies_quotidien.py` | 04 · Cas d'usage FHIR, étapes 1 à 4 complétées |

---

## Partie 01 · Premier DAG

**Journal de `charger`** : « Chargement de 300 lignes » (150 patients × 2 dans `transformer`).

**Tâche `controler`** ajoutée entre `transformer` et `charger` : elle lève une `ValueError` si n < 100.

**Avec `nb_patients = 40`, que voit-on dans Grid ?**
`transformer` renvoie 80, donc `controler` échoue (rouge, `ValueError`). `charger` passe en `upstream_failed` (orange) : elle n'est jamais lancée, car la tâche dont elle dépend a échoué. Airflow arrête la chaîne au premier contrôle raté, ce qui évite de charger des données non conformes.

---

## Partie 02 · Prévoir les exécutions

Paramètres : cron `0 6 * * *` (tous les jours à 6 h), `start_date` = 1er novembre 2026, `catchup=True`.

**1. Combien d'exécutions à l'activation (le 7 novembre, après 6 h) ? Quelle logical_date pour la première ?**
**7 exécutions**, une par jour du 1er au 7 novembre, car `catchup=True` rattrape tous les créneaux passés depuis `start_date`. La première a pour logical_date le **1er novembre 2026 à 06:00 UTC**.
Avec Airflow 3 (CronTriggerTimetable), la logical_date est l'heure du déclenchement : début et fin de l'intervalle sont identiques. Avec l'ancienne logique d'Airflow 2 (intervalles), il y en aurait eu 6, l'exécution du 7 attendant la fin de son intervalle le 8.

**3. Avec `catchup=False` et `dag_id = prevoir_executions_V2`, que crée l'activation ?**
Observé dans Airflow : l'activation a créé **une seule exécution planifiée**, `scheduled__2026-11-07T06:00:00+00:00`, c'est-à-dire le créneau le plus récent déjà passé. Les créneaux du 1er au 6 novembre n'ont **pas** été rattrapés.

Le renommage du `dag_id` sert à repartir d'un DAG neuf : sans lui, Airflow aurait gardé l'historique des 7 exécutions déjà créées.

Observations complémentaires :
- L'interface affiche « 07:00 » pour une exécution à 06:00 UTC : la `start_date` sans fuseau est lue en UTC, et l'interface affiche l'heure de Paris (+01:00).
- Au 21 novembre, le DAG compte **12 exécutions** : celle créée à l'activation, un lancement manuel, puis 10 exécutions planifiées entre le 8 et le 21 novembre. Les jours où mon ordinateur ou Docker était éteint, l'exécution n'a pas été rattrapée au redémarrage : c'est l'effet de `catchup=False`.

---

## Partie 03 · Fiabiliser

Cours, pas d'exercice. Les notions sont mises en œuvre dans la partie 04 :

| Notion | Où dans le DAG FHIR |
|---|---|
| Retries | `extraire` : 3 nouvelles tentatives, à 20 s d'écart |
| Callback | `alerter` (`on_failure_callback`) écrit l'alerte dans `donnees/alertes.log` |
| Idempotence | `charger` : DELETE des lignes du jour, puis INSERT |
| Validation | `valider` : règles vérifiées avec un schéma Pandera |

---

## Partie 04 · Cas d'usage : le flux FHIR quotidien

Chaîne : `extraire` (API FHIR) → `aplatir` (Parquet) → `valider` (Pandera) → `charger` (DuckDB).

**Étapes complétées :**
1. `extraire` : boucle de pagination ; on suit le lien `next` de chaque bundle jusqu'à la dernière page. `raise_for_status()` fait échouer la tâche en cas d'erreur HTTP, ce qui déclenche les retries.
2. `aplatir` : une ligne par Observation (id, patient, date, statut, valeur, unité), écrite en Parquet.
3. `valider` : une ligne est valide si elle a un patient, une valeur > 0 et une unité parmi g/L, mg/dL, mmol/L. Pandera vérifie ensuite que les lignes gardées respectent ce schéma.
4. `charger` : table `glycemies`, valeurs converties en mg/dL (g/L × 100 ; mmol/L × 18), suppression des lignes du jour puis insertion.

**Que faire des lignes invalides ?**
Les écarter du chargement, sans les supprimer : elles sont écrites à part dans un fichier `_rejets.parquet`, pour pouvoir les examiner. Ici, ce sont les examens annulés (statut `cancelled`, `dataAbsentReason`), qui n'ont ni valeur ni unité.

**Vérifications (4.4) :**

| Vérification | Résultat obtenu |
|---|---|
| Quatre tâches vertes (source = fichier) | ✅ `extraire`, `aplatir`, `valider`, `charger` en succès |
| Validation | 664 lignes valides, **20 rejetées** |
| Chargement | **664 glycémies chargées**, total dans l'entrepôt : 664 |
| Aucun doublon | Après une 2ᵉ exécution, `select count(*) from glycemies` renvoie toujours **664** |
| Panne (URL `baseR5x`, source = api) | `extraire` échoue avec `404 Not Found`, **4 essais** (1 + 3 retries) à 20 s d'écart, puis échec définitif ; l'alerte est écrite dans `alertes.log` ; les 3 tâches suivantes passent en « Échec en amont » |

L'URL a été remise sur `baseR4` après le test de panne.
