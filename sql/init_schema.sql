CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- DIMENSIONS — chargées une fois depuis le référentiel statique
-- (capteurs.csv, machines.csv, sites.csv), quasi jamais mises à jour
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_capteur (
    capteur_id          TEXT PRIMARY KEY,
    type_mesure         TEXT NOT NULL,
    plage_nominale_min  NUMERIC,
    plage_nominale_max  NUMERIC,
    fabricant           TEXT,
    date_installation   DATE,
    precision_capteur   NUMERIC
);

CREATE TABLE IF NOT EXISTS gold.dim_machine (
    machine_id             TEXT PRIMARY KEY,
    type_machine           TEXT,
    ligne_production       TEXT,
    criticite              TEXT,
    date_mise_service      DATE,
    capacite_nominale      INT,
    responsable_technique  TEXT,
    site_id                TEXT  -- rattachement machine -> site (extension utile pour le modèle)
);

CREATE TABLE IF NOT EXISTS gold.dim_site (
    site_id           TEXT PRIMARY KEY,
    nom               TEXT,
    region            TEXT,
    capacite_site     INT,
    fuseau_horaire    TEXT,
    responsable_site  TEXT
);

-- ============================================================
-- FAITS / AGRÉGATS — alimentés en continu par les jobs Spark Gold
-- (foreachBatch + MERGE INTO côté Delta, upsert ON CONFLICT côté Postgres)
-- ============================================================

-- Gold 1 : agrégation temporelle par machine, fenêtre glissante
CREATE TABLE IF NOT EXISTS gold.agg_fenetre_machine (
    machine_id      TEXT NOT NULL REFERENCES gold.dim_machine(machine_id),
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    valeur_moyenne  DOUBLE PRECISION,
    valeur_max      DOUBLE PRECISION,
    nb_mesures      INT,
    nb_anomalies    INT,
    PRIMARY KEY (machine_id, window_start)
);

-- Gold 2 : état courant par capteur (dernière valeur connue, dernier statut)
-- Mis à jour par MERGE INTO à CHAQUE nouvel événement — jamais par simple append.
CREATE TABLE IF NOT EXISTS gold.etat_courant_capteur (
    capteur_id                TEXT PRIMARY KEY REFERENCES gold.dim_capteur(capteur_id),
    machine_id                TEXT REFERENCES gold.dim_machine(machine_id),
    site_id                   TEXT REFERENCES gold.dim_site(site_id),
    derniere_valeur           DOUBLE PRECISION,
    derniere_unite            TEXT,
    dernier_statut_anomalie   BOOLEAN,
    derniere_qualite_signal   DOUBLE PRECISION,
    dernier_batterie_pct      INT,
    derniere_maj              TIMESTAMP
);

-- Index utiles pour les KPIs Power BI (filtrage par site / machine / statut)
CREATE INDEX IF NOT EXISTS idx_etat_courant_site ON gold.etat_courant_capteur(site_id);
CREATE INDEX IF NOT EXISTS idx_etat_courant_machine ON gold.etat_courant_capteur(machine_id);
CREATE INDEX IF NOT EXISTS idx_agg_fenetre_window ON gold.agg_fenetre_machine(window_start);
