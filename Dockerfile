# Una sola imagen para los nodos, el emisor externo y las pruebas: solo usan la biblioteca estándar.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY grupo/ grupo/
COPY tests/ tests/

RUN useradd --create-home --uid 1000 app
USER app

CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]
