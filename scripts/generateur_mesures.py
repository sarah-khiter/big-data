#!/usr/bin/env python3
"""
Générateur mock de mesures capteurs — publie vers Kafka (topic mesures).

Schéma JSON conforme à l'énoncé. Les IDs sont cohérents avec referentiel/*.csv
(cpt-001..024, m-01..06, site-lyon/site-nantes).

Injecte volontairement des anomalies (~taux configurable) pour tester Silver.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

ROOT = Path(__file__).resolve().parent.parent
REFERENTIEL = ROOT / "referentiel"

UNITES = {
    "temperature": "celsius",
    "vibration": "mm/s",
    "pression": "bar",
}


def load_capteurs() -> list[dict]:
    with open(REFERENTIEL / "capteurs.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_machine_sites() -> dict[str, str]:
    with open(REFERENTIEL / "machines.csv", newline="", encoding="utf-8") as f:
        return {row["machine_id"]: row["site_id"] for row in csv.DictReader(f)}


def capteur_to_machine(capteur_id: str) -> str:
    num = int(capteur_id.split("-")[1])
    return f"m-{(num - 1) // 4 + 1:02d}"


def ensure_topic(bootstrap: str, topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        print(f"Topic '{topic}' créé.")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


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
    # pression : hors plage haute ou basse
    if random.random() < 0.5:
        return round(hi + random.uniform(0.5, 3), 2)
    return round(max(0, lo - random.uniform(0.5, 2)), 2)


def build_event(capteur: dict, machine_sites: dict[str, str], taux_anomalie: float) -> dict:
    machine_id = capteur_to_machine(capteur["capteur_id"])
    site_id = machine_sites[machine_id]
    type_mesure = capteur["type_mesure"]
    lo = float(capteur["plage_nominale_min"])
    hi = float(capteur["plage_nominale_max"])

    if random.random() < taux_anomalie:
        valeur = valeur_anomalie(type_mesure, lo, hi)
    else:
        valeur = valeur_normale(type_mesure, lo, hi)

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
    parser = argparse.ArgumentParser(description="Générateur mock mesures capteurs → Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap (hôte)")
    parser.add_argument("--topic", default="mesures")
    parser.add_argument("--interval-min", type=float, default=1.0, help="Délai min entre mesures (s)")
    parser.add_argument("--interval-max", type=float, default=3.0, help="Délai max entre mesures (s)")
    parser.add_argument("--taux-anomalie", type=float, default=0.12, help="Fraction d'anomalies injectées (0-1)")
    parser.add_argument("--max-events", type=int, default=0, help="Arrêt après N événements (0 = infini)")
    args = parser.parse_args()

    capteurs = load_capteurs()
    machine_sites = load_machine_sites()
    ensure_topic(args.bootstrap, args.topic)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(
        f"Publication vers {args.bootstrap} topic={args.topic} | "
        f"{len(capteurs)} capteurs | taux anomalie={args.taux_anomalie:.0%} | "
        f"intervalle {args.interval_min}-{args.interval_max}s"
    )

    count = 0
    try:
        while True:
            capteur = random.choice(capteurs)
            event = build_event(capteur, machine_sites, args.taux_anomalie)
            producer.send(args.topic, value=event)
            producer.flush()
            count += 1
            if count % 50 == 0:
                print(f"  {count} événements publiés (dernier: {event['capteur_id']} valeur={event['valeur']})")
            if args.max_events and count >= args.max_events:
                print(f"Arrêt après {count} événements.")
                break
            time.sleep(random.uniform(args.interval_min, args.interval_max))
    except KeyboardInterrupt:
        print(f"\nArrêt manuel après {count} événements.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
