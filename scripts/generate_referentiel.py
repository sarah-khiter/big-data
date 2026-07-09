"""
Génère le référentiel statique (sites.csv, machines.csv, capteurs.csv)
cohérent avec les identifiants attendus par le générateur d'événements
(capteur_id: cpt-XXX, machine_id: m-XX, site_id: site-<nom>).

IMPORTANT : quand le générateur officiel sera fourni, comparer ses IDs à ceux
générés ici. S'ils diffèrent, ajuster SITES / MACHINES / CAPTEURS ci-dessous
puis relancer ce script — c'est la seule source de vérité du référentiel.
"""
import csv
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent.parent / "referentiel"
OUT_DIR.mkdir(exist_ok=True)

# --- Sites (2 usines) ---
SITES = [
    dict(site_id="site-lyon", nom="Lyon Usine 1", region="Auvergne-Rhône-Alpes",
         capacite_site=3000, fuseau_horaire="Europe/Paris", responsable_site="M. Bernard"),
    dict(site_id="site-nantes", nom="Nantes Usine 2", region="Pays de la Loire",
         capacite_site=1800, fuseau_horaire="Europe/Paris", responsable_site="L. Petit"),
]

# --- Machines (6, réparties 3/3 sur les 2 sites) ---
MACHINES = [
    dict(machine_id="m-01", type_machine="presse hydraulique", ligne_production="ligne-A",
         criticite="haute", date_mise_service="2021-06-01", capacite_nominale=500,
         responsable_technique="J. Dupont", site_id="site-lyon"),
    dict(machine_id="m-02", type_machine="convoyeur", ligne_production="ligne-A",
         criticite="moyenne", date_mise_service="2020-09-15", capacite_nominale=1200,
         responsable_technique="S. Martin", site_id="site-lyon"),
    dict(machine_id="m-03", type_machine="compresseur", ligne_production="ligne-B",
         criticite="haute", date_mise_service="2022-02-10", capacite_nominale=800,
         responsable_technique="J. Dupont", site_id="site-lyon"),
    dict(machine_id="m-04", type_machine="presse hydraulique", ligne_production="ligne-C",
         criticite="haute", date_mise_service="2021-11-20", capacite_nominale=500,
         responsable_technique="A. Lefevre", site_id="site-nantes"),
    dict(machine_id="m-05", type_machine="convoyeur", ligne_production="ligne-C",
         criticite="basse", date_mise_service="2019-05-03", capacite_nominale=1000,
         responsable_technique="A. Lefevre", site_id="site-nantes"),
    dict(machine_id="m-06", type_machine="compresseur", ligne_production="ligne-D",
         criticite="moyenne", date_mise_service="2023-01-17", capacite_nominale=900,
         responsable_technique="C. Rousseau", site_id="site-nantes"),
]

# --- Capteurs : 4 par machine (2 temperature, 1 vibration, 1 pression) = 24 capteurs ---
PLAGES = {
    "temperature": (10, 85, "celsius"),
    "vibration":   (0, 12, "mm/s"),
    "pression":    (1, 10, "bar"),
}
FABRICANTS = ["Siemens", "Bosch", "Schneider", "Honeywell"]

CAPTEURS = []
cpt_num = 1
for i, m in enumerate(MACHINES):
    plan = ["temperature", "temperature", "vibration", "pression"]
    for j, type_mesure in enumerate(plan):
        lo, hi, _unite = PLAGES[type_mesure]
        CAPTEURS.append(dict(
            capteur_id=f"cpt-{cpt_num:03d}",
            type_mesure=type_mesure,
            plage_nominale_min=lo,
            plage_nominale_max=hi,
            fabricant=FABRICANTS[(i + j) % len(FABRICANTS)],
            date_installation=f"202{2 + (cpt_num % 3)}-0{1 + (cpt_num % 9) % 9}-14",
            precision_capteur=round(0.1 + 0.1 * (cpt_num % 5), 2),
        ))
        cpt_num += 1

# --- Écriture des 3 CSV ---
def write_csv(name, rows):
    path = OUT_DIR / name
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} lignes écrites dans {path}")

write_csv("sites.csv", SITES)
write_csv("machines.csv", MACHINES)
write_csv("capteurs.csv", CAPTEURS)
