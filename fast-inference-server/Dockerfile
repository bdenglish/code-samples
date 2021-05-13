FROM python:3.9.4-buster

WORKDIR /opt/app-root
COPY requirements.txt ./


RUN echo "|--> Updating" \
    && pip install --no-cache-dir -r requirements.txt
COPY fast_inference_server/*.py ./
ENTRYPOINT ["gunicorn", "main:app", "--bind", "0.0.0.0:8080"]
#CMD ["-w", "4",  "-k", "uvicorn.workers.UvicornWorker"]