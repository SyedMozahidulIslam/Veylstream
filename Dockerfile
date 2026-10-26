# UNVERIFIED: Docker was not available where this project was built. Review before relying on it.
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY veylstream ./veylstream
RUN useradd -r -u 10001 veyl && mkdir -p /data && chown veyl /data
USER veyl
ENV VEYLSTREAM_DATA_DIR=/data VEYLSTREAM_HOST=0.0.0.0
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/api/v1/health/ready')"
CMD ["python", "-m", "veylstream", "serve"]
