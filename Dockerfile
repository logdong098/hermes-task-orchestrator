FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 10001 hermes \
    && mkdir -p /data /workspace \
    && chown -R hermes:hermes /data /workspace

USER hermes
EXPOSE 8000

CMD ["hermes-coordinator", "--host", "0.0.0.0", "--port", "8000"]
