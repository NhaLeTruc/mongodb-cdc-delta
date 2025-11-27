# Multi-stage Docker build for Delta Writer service

FROM python:3.11-slim as builder

WORKDIR /build

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY delta-writer/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


FROM python:3.11-slim

LABEL maintainer="MongoDB CDC Delta Pipeline"
LABEL description="Delta Writer - Kafka consumer to Delta Lake writer"

RUN groupadd -r deltawriter && useradd -r -g deltawriter deltawriter

WORKDIR /app

COPY --from=builder /root/.local /home/deltawriter/.local

COPY shared/ /app/shared/
COPY delta-writer/src/ /app/delta-writer/src/

RUN chown -R deltawriter:deltawriter /app

USER deltawriter

ENV PATH=/home/deltawriter/.local/bin:$PATH
ENV PYTHONPATH=/app:/app/delta-writer/src
ENV PYTHONUNBUFFERED=1

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

ENTRYPOINT ["python", "/app/delta-writer/src/main.py"]
