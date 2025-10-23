FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]