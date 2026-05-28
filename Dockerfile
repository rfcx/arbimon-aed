# Standalone (AWS-free) Audio Event Detection (clustering) worker container.
#
# Runs the worker as a single process that processes one job_id end-to-end
# (conductor recording-query + per-recording AED), instead of the AWS aed
# conductor Lambda + worker-Lambda fan-out. See aed_run_job.py.
#
# Reads config from env (no AWS Secrets Manager):
#   host, port, schema, tm_driver_pwd, ARBIMON_DB_USER  (DB)
#   S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (S3; endpoint
#     optional), RECBUCKET, WRITEBUCKET, AWS_SECRET (S3 key path prefix)
#
# numpy<1.24 pinned for parity with the legacy numerical code.
FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libsndfile1 ffmpeg && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
      numpy==1.22.4 scipy==1.8.1 scikit-image==0.19.3 soundfile==0.12.1 \
      sqlalchemy==1.4.52 mysql-connector-python==8.1.0 boto3==1.28.71

WORKDIR /app
COPY functions/worker/ /app/
# worker/secrets.py (AWS Secrets Manager helper) shadows Python's stdlib
# `secrets`, which numpy imports transitively. db.py no longer needs it.
RUN rm -f /app/secrets.py
COPY aed_run_job.py /app/aed_run_job.py

ENTRYPOINT ["python3", "/app/aed_run_job.py"]