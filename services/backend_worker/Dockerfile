FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1     PYTHONDONTWRITEBYTECODE=1     PYTHONPATH=/app

COPY services/ /app/services/

RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser     && chown -R appuser:appuser /app

USER appuser

EXPOSE 8001

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3     CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health', timeout=2)" || exit 1

CMD ["python3", "-m", "services.backend_worker.main"]
