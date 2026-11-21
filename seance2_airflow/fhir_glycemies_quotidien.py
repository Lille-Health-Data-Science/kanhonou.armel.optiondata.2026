"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path(__file__).resolve().parents[1] / "donnees"
BASE = os.environ.get("FHIR_BASE", "https://hapi.fhir.org/baseR4")   # panne volontaire (TD.md, 4.4) : mettre une mauvaise URL
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []

        # Étape 1 : on lit la réponse page par page, tant qu'il existe une page suivante
        while url:
            r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
            r.raise_for_status()                      # erreur HTTP → la tâche échoue → retry
            bundle = r.json()

            # on garde les ressources de la page
            for entree in bundle.get("entry", []):
                ressources.append(entree["resource"])

            # on cherche le lien « next » ; s'il n'y en a pas, url = None et la boucle s'arrête
            url = None
            for lien in bundle.get("link", []):
                if lien["relation"] == "next":
                    url = lien["url"]

            requete = None                            # le lien « next » contient déjà les paramètres

        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données

    @task
    def aplatir(chemin: str) -> str:
        """Étape 2 : une ligne par Observation (id, patient, date, statut, valeur, unite), en Parquet."""
        import pandas as pd

        lignes = []
        for texte in Path(chemin).read_text().splitlines():
            if not texte.strip():
                continue                                  # ignore les lignes vides
            o = json.loads(texte)                         # une Observation FHIR
            q = o.get("valueQuantity") or {}              # vide si l'examen est annulé
            lignes.append({
                "id": o.get("id"),
                "patient": (o.get("subject") or {}).get("reference"),
                "date": o.get("effectiveDateTime"),
                "statut": o.get("status"),
                "valeur": q.get("value"),
                "unite": q.get("unit"),
            })

        df = pd.DataFrame(lignes, columns=["id", "patient", "date", "statut", "valeur", "unite"])
        sortie = Path(chemin).with_suffix(".parquet")
        df.to_parquet(sortie, index=False)
        print(len(df), "lignes ; unités :", df["unite"].value_counts(dropna=False).to_dict())
        return str(sortie)

    @task
    def valider(chemin: str) -> str:
        """Étape 3 : schéma Pandera ; que faire des lignes invalides ?"""
        import pandas as pd
        import pandera.pandas as pa

        df = pd.read_parquet(chemin)

        # 1. Le tri : une ligne est valide si elle a un patient, une valeur > 0 et une unité connue
        regle = df["patient"].notna() & (df["valeur"] > 0) & df["unite"].isin(UNITES)
        valides = df[regle]
        rejets = df[~regle]                            # ~ veut dire « l'inverse de »

        # 2. Le contrôle Pandera : on vérifie que les lignes gardées respectent bien le schéma
        schema = pa.DataFrameSchema({
            "patient": pa.Column(str, nullable=False),
            "valeur": pa.Column(float, pa.Check.gt(0), coerce=True),
            "unite": pa.Column(str, pa.Check.isin(UNITES)),
        })
        schema.validate(valides)                       # lève une erreur si une ligne est hors règle

        # 3. On écrit les lignes valides ; les rejets sont gardés à part pour pouvoir les examiner
        sortie = Path(chemin).with_name(Path(chemin).stem + "_valides.parquet")
        valides.to_parquet(sortie, index=False)
        rejets.to_parquet(Path(chemin).with_name(Path(chemin).stem + "_rejets.parquet"), index=False)
        print(len(valides), "lignes valides,", len(rejets), "lignes rejetées")
        return str(sortie)

    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Étape 4 : DELETE + INSERT du jour dans donnees/entrepot.duckdb, donc rejouable sans doublon."""
        import duckdb

        j = jour(data_interval_start)                  # le jour traité, ex. "2026-11-20"
        con = duckdb.connect(str(DONNEES / "entrepot.duckdb"))

        # la table est créée une seule fois (au premier lancement)
        con.execute("""
            CREATE TABLE IF NOT EXISTS glycemies (
                id VARCHAR, patient VARCHAR, date VARCHAR, statut VARCHAR,
                valeur_mg_dl DOUBLE, jour DATE
            )
        """)

        # idempotence : on supprime d'abord les lignes de ce jour, puis on les réinsère
        con.execute("DELETE FROM glycemies WHERE jour = CAST(? AS DATE)", [j])
        con.execute("""
            INSERT INTO glycemies
            SELECT id, patient, date, statut,
                   CASE unite
                       WHEN 'g/L'    THEN valeur * 100   -- 1 g/L = 100 mg/dL
                       WHEN 'mmol/L' THEN valeur * 18    -- 1 mmol/L ≈ 18 mg/dL
                       ELSE valeur                       -- déjà en mg/dL
                   END,
                   CAST(? AS DATE)
            FROM read_parquet(?)
        """, [j, chemin])

        chargees = con.execute("SELECT count(*) FROM glycemies WHERE jour = CAST(? AS DATE)", [j]).fetchone()[0]
        total = con.execute("SELECT count(*) FROM glycemies").fetchone()[0]
        con.close()
        print(chargees, "glycémies chargées pour le", j, "; total dans l'entrepôt :", total)

    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()