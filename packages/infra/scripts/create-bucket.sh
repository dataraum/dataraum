#!/bin/sh
# Create the DuckLake bucket in SeaweedFS (one-shot, run by the seaweedfs-init
# service once the S3 gateway is healthy). SeaweedFS does not reliably
# auto-create a bucket on first PutObject, so the engine + cockpit gate on this
# job's successful completion before they ATTACH the lake — mirroring the
# temporal-create-namespace one-shot. The dev SeaweedFS is ephemeral (no
# volume), so the bucket is created fresh on every `up`. Idempotent: an
# already-existing bucket is treated as success.
set -eu

S3_BUCKET=${S3_BUCKET:-dataraum-lake}
SEAWEEDFS_MASTER=${SEAWEEDFS_MASTER:-seaweedfs:9333}
SEAWEEDFS_FILER=${SEAWEEDFS_FILER:-seaweedfs:8888}
MAX_ATTEMPTS=${S3_INIT_MAX_ATTEMPTS:-30}
SLEEP_SECONDS=${S3_INIT_SLEEP_SECONDS:-2}

FILER_HOST=$(echo "$SEAWEEDFS_FILER" | cut -d: -f1)
FILER_PORT=$(echo "$SEAWEEDFS_FILER" | cut -d: -f2)

echo 'Waiting for SeaweedFS filer port to be available...'
attempt=1
while ! nc -z -w 10 "$FILER_HOST" "$FILER_PORT"; do
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "SeaweedFS filer port did not become available after $MAX_ATTEMPTS attempts"
    exit 1
  fi
  echo "Filer port not ready yet, waiting... (attempt $attempt/$MAX_ATTEMPTS)"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECONDS"
done
echo 'SeaweedFS filer port is available'

echo "Creating bucket '$S3_BUCKET'..."
attempt=1
while :; do
  # `weed shell` reads commands from stdin; piping suppresses the prompt. It
  # talks to the master (-master) + filer (-filer). s3.bucket.create is a no-op
  # if the bucket already exists.
  out=$(echo "s3.bucket.create -name $S3_BUCKET" \
    | weed shell -master="$SEAWEEDFS_MASTER" -filer="$SEAWEEDFS_FILER" 2>&1 || true)
  echo "$out"

  if echo "$S3_BUCKET" | grep -q .; then
    # Confirm the bucket now exists (idempotent + robust to create's exit code).
    listed=$(echo 's3.bucket.list' \
      | weed shell -master="$SEAWEEDFS_MASTER" -filer="$SEAWEEDFS_FILER" 2>&1 || true)
    if echo "$listed" | grep -q "$S3_BUCKET"; then
      echo "Bucket '$S3_BUCKET' ready"
      break
    fi
  fi

  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "Failed to create bucket '$S3_BUCKET' after $MAX_ATTEMPTS attempts"
    exit 1
  fi
  echo "Bucket not ready yet, waiting... (attempt $attempt/$MAX_ATTEMPTS)"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECONDS"
done
