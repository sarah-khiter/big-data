# Étend l'image officielle Spark pour figer les dépendances Python
# nécessaires à Delta Lake + l'écriture Postgres (upsert Gold).
# Évite de refaire `pip install` à la main à chaque recréation du conteneur
# (docker compose down/up), contrairement à un `docker exec -u root pip install`
# qui ne survit pas à la suppression du conteneur.
FROM apache/spark:3.5.1-python3

USER root
RUN pip install --no-cache-dir delta-spark==3.2.0 psycopg2-binary
# Pas de retour à un USER spécifique : on garde le comportement par défaut
# de l'image (le conteneur tournera comme avant, seul l'ajout des paquets
# Python change).
