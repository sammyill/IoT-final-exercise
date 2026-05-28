# ============================================================
# Dockerfile — Immagine del sensore IoT
# ============================================================
# Usa Python slim (immagine minimale senza pacchetti superflui).
# Installa le dipendenze prima di copiare il codice sorgente:
# questo sfrutta la cache di Docker e velocizza le rebuild.
# ============================================================

FROM python:3.11-slim

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Copia prima il file dei requisiti (sfrutta la cache di Docker)
COPY requirements.txt .

# Installa le dipendenze Python senza cache locale (risparmia spazio)
RUN pip install --no-cache-dir -r requirements.txt

# Ora copia il codice sorgente del sensore
COPY sensor.py .

# -u = unbuffered stdout/stderr: i log appaiono subito in docker logs
CMD ["python", "-u", "sensor.py"]
