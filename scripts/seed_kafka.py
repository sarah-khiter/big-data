"""
Générateur de secours pour peupler Kafka sans dépendre de `kafka-python`
(cassé sous Windows, cf. README.md Incident 3 / README_role_C.md Incident 3).

Construit des événements JSON conformes au schéma de l'énoncé (mêmes IDs que
`referentiel/*.csv`) et les écrit sur stdout, une ligne par événement — à
piper dans `kafka-console-producer.sh` (voir `scripts/run_all.sh`).

Sur Linux/Mac, préférez le vrai générateur (`scripts/generateur_mesures.py`,
rôle B), qui simule un flux continu avec un intervalle entre mesures. Celui-ci
est un utilitaire de seed/démo, pas un remplacement.
"""
from __future__ import annotations

import csv
import json
import random
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REFERENTIEL = ROOT / "referentiel"

UNITES = {"temperature": "celsius", "vibration": "mm/s", "pression": "bar"}


def load_capteurs() -> list[dict]:
    with open(REFERENTIEL / "capteurs.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_machine_sites() -> dict[str, str]:
    with open(REFERENTIEL / "machines.csv", newline="", encoding="utf-8") as f:
        return {r["machine_id"]: r["site_id"] for r in csv.DictReader(f)}


def capteur_to_machine(capteur_id: str) -> str:
    num = int(capteur_id.split("-")[1])
    return f"m-{(num - 1) // 4 + 1:02d}"


def valeur_normale(type_mesure: str, lo: float, hi: float) -> float:
    mid = (lo + hi) / 2
    spread = (hi - lo) * 0.15
    if type_mesure == "temperature":
        return round(random.gauss(mid, spread), 1)
    if type_mesure == "vibration":
        return round(abs(random.gauss(mid * 0.4, spread * 0.3)), 2)
    return round(random.uniform(lo + 0.5, hi - 0.5), 2)


def valeur_anomalie(type_mesure: str, lo: float, hi: float) -> float:
    if type_mesure == "temperature":
        return round(hi + random.uniform(5, 25), 1)
    if type_mesure == "vibration":
        return round(hi + random.uniform(1, 8), 2)
    if random.random() < 0.5:
        return round(hi + random.uniform(0.5, 3), 2)
    return round(max(0, lo - random.uniform(0.5, 2)), 2)


def build_event(capteur: dict, machine_sites: dict[str, str], taux_anomalie: float) -> dict:
    machine_id = capteur_to_machine(capteur["capteur_id"])
    site_id = machine_sites[machine_id]
    type_mesure = capteur["type_mesure"]
    lo = float(capteur["plage_nominale_min"])
    hi = float(capteur["plage_nominale_max"])
    valeur = (
        valeur_anomalie(type_mesure, lo, hi)
        if random.random() < taux_anomalie
        else valeur_normale(type_mesure, lo, hi)
    )
    return {
        "event_id": f"evt-{uuid.uuid4().hex[:8]}",
        "capteur_id": capteur["capteur_id"],
        "machine_id": machine_id,
        "site_id": site_id,
        "type_mesure": type_mesure,
        "valeur": valeur,
        "unite": UNITES[type_mesure],
        "qualite_signal": round(random.uniform(0.85, 1.0), 2),
        "batterie_pourcentage": random.randint(20, 100),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{random.randint(0, 999):03d}Z",
    }


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    taux_anomalie = float(sys.argv[2]) if len(sys.argv) > 2 else 0.15

    capteurs = load_capteurs()
    machine_sites = load_machine_sites()

    for _ in range(n):
        capteur = random.choice(capteurs)
        event = build_event(capteur, machine_sites, taux_anomalie)
        sys.stdout.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    main()
