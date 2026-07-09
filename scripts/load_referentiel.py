"""
Charge le référentiel statique (sites.csv, machines.csv, capteurs.csv) dans
les tables de dimension Postgres (gold.dim_site, gold.dim_machine, gold.dim_capteur).

À exécuter UNE FOIS après `docker compose up` et l'application de init_schema.sql
(et à chaque fois que le référentiel change, ce qui doit rester rare).

Usage :
    pip install psycopg2-binary
    python3 scripts/load_referentiel.py
"""
import csv
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

REF_DIR = Path(__file__).resolve().parent.parent / "referentiel"

PG_CONN = dict(
    host="localhost", port=5432,
    dbname="capteurs", user="capteurs", password="capteurs",
)


def read_csv(name):
    with open(REF_DIR / name, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_sites(conn):
    rows = read_csv("sites.csv")
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO gold.dim_site (site_id, nom, region, capacite_site, fuseau_horaire, responsable_site)
            VALUES %s
            ON CONFLICT (site_id) DO UPDATE SET
                nom = EXCLUDED.nom, region = EXCLUDED.region,
                capacite_site = EXCLUDED.capacite_site,
                fuseau_horaire = EXCLUDED.fuseau_horaire,
                responsable_site = EXCLUDED.responsable_site
        """, [(r["site_id"], r["nom"], r["region"], int(r["capacite_site"]),
               r["fuseau_horaire"], r["responsable_site"]) for r in rows])
    print(f"[dim_site] {len(rows)} lignes upsertées")


def load_machines(conn):
    rows = read_csv("machines.csv")
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO gold.dim_machine
                (machine_id, type_machine, ligne_production, criticite,
                 date_mise_service, capacite_nominale, responsable_technique, site_id)
            VALUES %s
            ON CONFLICT (machine_id) DO UPDATE SET
                type_machine = EXCLUDED.type_machine,
                ligne_production = EXCLUDED.ligne_production,
                criticite = EXCLUDED.criticite,
                date_mise_service = EXCLUDED.date_mise_service,
                capacite_nominale = EXCLUDED.capacite_nominale,
                responsable_technique = EXCLUDED.responsable_technique,
                site_id = EXCLUDED.site_id
        """, [(r["machine_id"], r["type_machine"], r["ligne_production"], r["criticite"],
               r["date_mise_service"], int(r["capacite_nominale"]),
               r["responsable_technique"], r["site_id"]) for r in rows])
    print(f"[dim_machine] {len(rows)} lignes upsertées")


def load_capteurs(conn):
    rows = read_csv("capteurs.csv")
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO gold.dim_capteur
                (capteur_id, type_mesure, plage_nominale_min, plage_nominale_max,
                 fabricant, date_installation, precision_capteur)
            VALUES %s
            ON CONFLICT (capteur_id) DO UPDATE SET
                type_mesure = EXCLUDED.type_mesure,
                plage_nominale_min = EXCLUDED.plage_nominale_min,
                plage_nominale_max = EXCLUDED.plage_nominale_max,
                fabricant = EXCLUDED.fabricant,
                date_installation = EXCLUDED.date_installation,
                precision_capteur = EXCLUDED.precision_capteur
        """, [(r["capteur_id"], r["type_mesure"], float(r["plage_nominale_min"]),
               float(r["plage_nominale_max"]), r["fabricant"], r["date_installation"],
               float(r["precision_capteur"])) for r in rows])
    print(f"[dim_capteur] {len(rows)} lignes upsertées")


def main():
    conn = psycopg2.connect(**PG_CONN)
    try:
        load_sites(conn)
        load_machines(conn)
        load_capteurs(conn)
        conn.commit()
        print("Référentiel chargé avec succès.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
