FROM apache/spark:4.0.1-python3

USER root
RUN python3 -m pip install --no-cache-dir requests kafka-python python-dotenv spacy==3.8.10 \
    && python3 -m spacy download en_core_web_sm \
    && mkdir -p /app/state /app/checkpoints \
    && chown -R spark:spark /app

WORKDIR /app
COPY --chown=spark:spark scripts/ /app/scripts/
USER spark
