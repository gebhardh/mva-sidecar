FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mva_sidecar/ ./mva_sidecar/

RUN useradd -r -u 1000 sidecar && \
    mkdir -p /etc/mva-sidecar && \
    chown -R sidecar:sidecar /etc/mva-sidecar

USER sidecar

EXPOSE 9100

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9100/metrics', timeout=3)" || exit 1

ENTRYPOINT ["python", "-m", "mva_sidecar.service"]
CMD ["--config", "/etc/mva-sidecar/config.yaml"]
