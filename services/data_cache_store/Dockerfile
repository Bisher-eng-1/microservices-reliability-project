# Service D: data-cache-store
FROM python:3.11-slim

WORKDIR /app

# Ensure unbuffered output for clean Docker logging
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy source tree
COPY services/ /app/services/

# Create non-root system user for security
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8002

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8002/health', timeout=2)" || exit 1

CMD ["python3", "-m", "services.data_cache_store.main"]
