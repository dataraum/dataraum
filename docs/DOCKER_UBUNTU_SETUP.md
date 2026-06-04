# Docker Ubuntu Setup (behind a corporate proxy)

How to bring the full DataRaum stack up on an Ubuntu host that sits **behind an
HTTP proxy** and has a **small root disk**. Written from a real bring-up on
`shv-18` (Ubuntu 24.04, proxy `10.2.1.198:8080`, 22 GB root LVM, containerd
snapshotter). Each section is a problem we hit, the root cause, and the fix.

If you just want the happy path, jump to [Quickstart](#quickstart) at the bottom.

---

## Environment assumptions

- Docker Engine with **BuildKit** + the **containerd image store** (images and
  build cache live under `/var/lib/containerd`, not `/var/lib/docker`).
- No direct internet — all outbound traffic goes through an **HTTP proxy**
  (`http://10.2.1.198:8080` here; substitute yours everywhere below).
- Commands run with `sudo` (the proxy/DNS config lives under `/root` and
  `/etc`).

Two layers need the proxy, and they are configured **separately**:

| Layer | Used for | Configured in |
|-------|----------|---------------|
| **Docker daemon** | pulling base images / manifests | `/etc/systemd/system/docker.service.d/http-proxy.conf` |
| **Docker client / builds** | `RUN` steps (apt, uv, bun), and `docker run`/compose containers | `/root/.docker/config.json` (`proxies`) |

Getting one but not the other is the source of most of the failures below.

---

## Problem 1 — BuildKit can't fetch image metadata (DNS / proxy)

### Symptom
```
target cockpit: failed to solve: DeadlineExceeded: failed to fetch anonymous token:
Get "https://auth.docker.io/token?...": dial tcp: lookup auth.docker.io: i/o timeout
```

### Root cause
The **daemon** had the proxy (so `docker pull` worked), but the **client/build
layer** did not, so BuildKit's manifest fetch tried to reach Docker Hub directly
and timed out. On hosts using `systemd-resolved`, BuildKit also can't always
reach the `127.0.0.53` stub resolver.

### Fix
Give the daemon explicit DNS, and give the **client** the proxy so it's injected
into builds.

```bash
# 1. Daemon DNS (so the daemon/buildkit don't depend on the systemd-resolved stub)
sudo tee /etc/docker/daemon.json >/dev/null <<'EOF'
{
  "dns": ["8.8.8.8", "1.1.1.1"]
}
EOF
sudo systemctl restart docker

# 2. Client/build proxy — injected into RUN steps and containers.
#    noProxy MUST list every compose service name + localhost so internal
#    traffic never goes through the proxy.
sudo mkdir -p /root/.docker
sudo tee /root/.docker/config.json >/dev/null <<'EOF'
{
  "proxies": {
    "default": {
      "httpProxy": "http://10.2.1.198:8080",
      "httpsProxy": "http://10.2.1.198:8080",
      "noProxy": "localhost,127.0.0.1,::1,172.16.0.0/12,postgres,seaweedfs,seaweedfs-init,temporal,temporal-admin-tools,temporal-create-namespace,temporal-ui,engine-worker,cockpit"
    }
  }
}
EOF
```

The daemon proxy itself was already present at
`/etc/systemd/system/docker.service.d/http-proxy.conf`:
```ini
[Service]
Environment="HTTP_PROXY=http://10.2.1.198:8080"
Environment="HTTPS_PROXY=http://10.2.1.198:8080"
Environment="NO_PROXY=localhost,127.0.0.1,::1"
```

### Verify
```bash
# A container can now reach Docker Hub through the proxy (expect 200, not 000):
sudo docker run --rm curlimages/curl -sS -m 15 \
  "https://auth.docker.io/token?service=registry.docker.io" -o /dev/null -w "%{http_code}\n"
```

---

## Problem 2 — Manifest DNS timeout for an *uncached* base image

### Symptom
```
target cockpit: failed to solve: failed to fetch anonymous token:
Get "https://auth.docker.io/token?scope=repository%3Aoven%2Fbun%3Apull&service=registry.docker.io":
dial tcp: lookup auth.docker.io on 127.0.0.53:53: i/o timeout
```
`oven/bun:1` resolved instantly (cached) but `oven/bun:1-slim` failed — because
BuildKit had to resolve its manifest fresh and the resolver path was flaky.

### Root cause
BuildKit's manifest resolver doesn't reliably honor the proxy/DNS the daemon
uses. The **daemon's** `docker pull` does (it goes through the proxy).

### Fix — pre-pull base images through the daemon
```bash
sudo docker pull oven/bun:1-slim
sudo docker pull oven/bun:1
sudo docker pull python:3.14-slim
sudo docker pull ghcr.io/astral-sh/uv:latest
```
Once an image is in the local store, BuildKit reads its metadata locally
(`0.0s`) instead of resolving over the network.

---

## Problem 3 — DuckDB extension install hangs during build

### Symptom
The engine-worker build hangs (then times out) on:
```
RUN /app/.venv/bin/python -c "import duckdb; ... c.execute('INSTALL ducklake') ..."
```

### Root cause
`apt` and `uv` honor the `HTTP_PROXY` env injected into the build, but
**DuckDB's extension installer uses its own HTTP client and ignores
`HTTP_PROXY`** — so `INSTALL ducklake` / `INSTALL httpfs` try to reach
`extensions.duckdb.org` directly, with no route, and hang.

### Fix (committed)
`packages/engine/docker/worker.Dockerfile` reads the proxy from the build env
and passes it to DuckDB explicitly via `SET http_proxy`. No-op when there's no
proxy, so air-gapped builds are unaffected:

```dockerfile
RUN /app/.venv/bin/python -c "import os, duckdb; \
    c = duckdb.connect(); \
    c.execute(\"SET extension_directory = '/opt/dataraum/duckdb-extensions'\"); \
    _p = (os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy') or '').replace('http://', '').replace('https://', '').rstrip('/'); \
    c.execute(\"SET http_proxy = '\" + _p + \"'\") if _p else None; \
    c.execute('INSTALL ducklake'); \
    c.execute('LOAD ducklake'); \
    c.execute('INSTALL httpfs'); \
    c.execute('LOAD httpfs'); \
    c.close()"
```

---

## Problem 4 — `no space left on device`

### Symptom
```
failed to extract layer ...: write /var/lib/containerd/.../scipy/...so:
no space left on device
```

### Root cause
A 22 GB root disk, 100% full. Most of it was Docker **build cache** (~3.8 GB)
plus images, all under `/var/lib/containerd`.

### Fix — reclaim space (safe; does NOT touch named volumes / data)
```bash
df -h /                         # confirm the root fs is full
sudo docker system df           # see what's using space
sudo docker builder prune -af   # build cache — usually the biggest reclaim
sudo docker image prune -af     # dangling/unused images
sudo docker container prune -f  # stopped containers from failed runs

# system logs can also eat several GB:
sudo journalctl --vacuum-size=200M
```

> ⚠️ **Do NOT** add `--volumes` to a prune unless you're sure — it deletes
> Postgres / SeaweedFS / DuckLake data. The cache/image prune above is almost
> always enough.

### Note: 22 GB is too small for this stack
Images alone are ~5 GB, plus data volumes that grow with use. If the volume
group has free extents (`sudo vgs` → `VFree`), grow it:
```bash
sudo lvextend -l +100%FREE /dev/mapper/ubuntu--vg-ubuntu--lv
sudo resize2fs /dev/mapper/ubuntu--vg-ubuntu--lv
```
If `VFree` is 0 (our case) the **virtual disk** itself must be enlarged in the
hypervisor first, then:
```bash
sudo growpart /dev/sda 3
sudo pvresize /dev/sda3
sudo lvextend -l +100%FREE /dev/mapper/ubuntu--vg-ubuntu--lv
sudo resize2fs /dev/mapper/ubuntu--vg-ubuntu--lv
```

---

## Problem 5 — SeaweedFS stays `unhealthy` (healthcheck 403)

### Symptom
```
✘ Container infra-seaweedfs-1  ... is unhealthy
# inspect shows:
"Output":"wget: server returned error: HTTP/1.1 403 OK"
```
SeaweedFS itself started fine (master + volume + filer + S3 all up in the logs)
— only the **healthcheck** failed.

### Root cause
The `/root/.docker/config.json` proxy injects `HTTP_PROXY` into **every runtime
container**, not just builds. SeaweedFS's healthcheck uses busybox `wget`, which
honors `HTTP_PROXY` but **ignores `NO_PROXY`** — so `wget http://localhost:8888/`
got routed through the proxy, which returned 403.

Diagnosis that nailed it:
```bash
sudo docker exec infra-seaweedfs-1 sh -c 'wget -S -O /dev/null http://localhost:8888/ 2>&1 | head -3'
# -> Connecting to 10.2.1.198:8080  (the PROXY, not localhost)
#    HTTP/1.1 403 OK
```
Only the healthcheck was affected — `seaweedfs-init` uses `nc` + `weed shell`
(gRPC), and the engine/cockpit HTTP clients honor `NO_PROXY` properly. The
engine *needs* the runtime proxy (to reach `api.anthropic.com`), so the fix is
**not** to remove the proxy — it's to make the healthcheck bypass it.

### Fix (committed)
`packages/infra/docker-compose.yml`, SeaweedFS healthcheck — clear proxy env for
the probe (it only ever hits localhost):
```yaml
test: ["CMD-SHELL", "HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= wget -q -O /dev/null http://localhost:8888/ || exit 1"]
```

---

## Problem 6 — Cockpit uploads 503 through the proxy (`Bun.S3Client` ignores `NO_PROXY`)

### Symptom
Uploads fail with a 500; the cockpit log shows an S3 call wrapped around an HTML
proxy error:
```
S3Error: <HTML><TITLE>503 Service Unavailable</TITLE>
<H1>503 Service Unavailable</H1>
Failed to resolve the name of server <B>seaweedfs</B></HTML>
  at listPrefixObjects (...)  at handleUpload (...)
```
SeaweedFS's S3 gateway returns **XML** errors — a plain **HTML** 503 is the
**proxy** talking. "Failed to resolve the name of server seaweedfs" = the proxy
being handed an internal docker name it can't resolve.

### Root cause
The cockpit uses **`Bun.S3Client`** (`packages/cockpit/src/upload/s3-upload.ts`).
`Bun.S3Client` honors `HTTP_PROXY` but **ignores `NO_PROXY`** — proven: `seaweedfs`
is present in BOTH `NO_PROXY` and `no_proxy` in the container, yet the S3 call
still went through the proxy. So the `config.json` runtime proxy injection routes
the cockpit's INTERNAL object-store traffic through the corporate proxy, which
503s it. `Bun.S3Client` has no per-client `proxy` option, so there's no env-level
or client-level escape.

### Fix (committed code + host-local override)
Run the cockpit with **no global proxy** (so `Bun.S3Client`, Postgres, Temporal
all go direct) and apply the proxy **only** to the Anthropic call via Bun's
per-request `fetch(url, { proxy })`:

1. `packages/cockpit/src/outbound-proxy.ts` (committed) — a server-only side-effect
   module: installs a global `fetch` wrapper that adds `{ proxy: OUTBOUND_PROXY }`
   only for `*.anthropic.com`. No-op unless `OUTBOUND_PROXY` is set. `Bun.S3Client`
   is native and never uses global `fetch`, so it always stays direct.
2. `packages/cockpit/src/config.ts` (committed) — `import "./outbound-proxy";` at
   the top so the shim installs at server boot.
3. `packages/infra/docker-compose.override.yml` (**host-local, not committed**) —
   strip the cockpit's global proxy and hand it the proxy under the non-standard
   name the shim reads:
   ```yaml
   services:
     cockpit:
       environment:
         HTTP_PROXY: ""
         HTTPS_PROXY: ""
         http_proxy: ""
         https_proxy: ""
         OUTBOUND_PROXY: "http://10.2.1.198:8080"
   ```

> ⚠️ **`-f` disables override auto-loading.** Compose only auto-includes
> `docker-compose.override.yml` when you pass NO `-f`. Because every command here
> uses `-f packages/infra/docker-compose.yml`, you MUST pass the override
> explicitly too, on **every** command:
> ```bash
> sudo docker compose \
>   -f packages/infra/docker-compose.yml \
>   -f packages/infra/docker-compose.override.yml \
>   up -d --no-deps cockpit
> ```

### Verify
```bash
sudo docker compose -f packages/infra/docker-compose.yml -f packages/infra/docker-compose.override.yml \
  exec cockpit sh -c 'env | grep -i proxy'
# HTTP_PROXY must be EMPTY; OUTBOUND_PROXY=http://10.2.1.198:8080
```
Uploads (Bun.S3Client → seaweedfs direct) and chat (Anthropic → via OUTBOUND_PROXY)
then both work.

---

## Problem 7 — Uploads 500 because SeaweedFS can't write (disk full)

### Symptom
With the proxy fixed, uploads still 500. SeaweedFS logs:
```
disk_location.go:561 dir /data disk free 0.00% < required 1.00%
master_grpc_server_volume.go:105 volume grow request ... failed: only 0 volumes left, not enough for 2
# and:
/dev/mapper/ubuntu--vg-ubuntu--lv  22G  21G  0  100%  /
```

### Root cause
The root disk is 100% full. SeaweedFS requires **≥1% free** to allocate a volume
and **refuses every write** below that. The dev SeaweedFS is ephemeral (no
volume), so its `/data` is the host disk — when the host fills, writes stop.

### Fix
Free space, then (if needed) grow the disk — see Problem 4 for the full toolkit.
The minimum to get SeaweedFS writing again:
```bash
sudo docker builder prune -af                 # reclaim build cache
df -h /
# still ~0 free? fold in the spare disk for +2GB (no reboot):
sudo blkid /dev/sdb                            # no output = empty → safe
sudo pvcreate /dev/sdb && sudo vgextend ubuntu-vg /dev/sdb
sudo lvextend -l +100%FREE /dev/ubuntu-vg/ubuntu-lv && sudo resize2fs /dev/ubuntu-vg/ubuntu-lv
```
Once `df` shows ≥1% free, the `disk free 0.00% < required 1.00%` line stops and
uploads succeed. The durable fix is growing the VM's `sda` in the hypervisor.

> ⚠️ **Do NOT use `docker image prune -a` on this box.** The `-a` removes every
> image not backing a running container — including the **pre-pulled base images**
> (`oven/bun:1-slim`, `ghcr.io/astral-sh/uv:latest`, `python:3.14-slim`). With
> them gone, the next build hits the BuildKit manifest-fetch timeout again
> (Problem 2). Use `docker builder prune -af` (cache only) to reclaim space.

---

## Problem 8 — engine-worker crash-loops on DuckLake bootstrap (`postgres_scanner` download)

### Symptom
```
RuntimeError: DuckLake bootstrap failed (catalog_url=postgresql://...:5432/dataraum_lake_catalog,
  data_path=s3://dataraum-lake/lake): ... Failed to download extension "postgres_scanner" at URL
  "http://extensions.duckdb.org/v1.5.2/linux_amd64/postgres_scanner.duckdb_extension.gz"
  (ERROR Connection timed out)
```

### Root cause
The DuckLake bootstrap runs `ATTACH 'ducklake:postgres:...'`, which makes DuckDB
**auto-install the `postgres_scanner` extension** to reach the Postgres catalog.
The worker image pre-bakes `ducklake` + `httpfs` but **not** `postgres_scanner`,
so at runtime DuckDB tries to download it — and DuckDB ignores `HTTP_PROXY`
(same as Problem 3), so it times out behind a proxy / fails air-gapped.

### Fix (committed)
`packages/engine/docker/worker.Dockerfile` pre-bakes `postgres_scanner` at build
time alongside the other two:
```dockerfile
    c.execute('INSTALL httpfs'); \
    c.execute('LOAD httpfs'); \
    c.execute('INSTALL postgres_scanner'); \
    c.execute('LOAD postgres_scanner'); \
    c.close()"
```
The runtime `ATTACH` then finds it cached in the image instead of downloading.

---

## Problem 9 — engine-worker can't connect to Temporal (down, or internal gRPC proxied)

### Symptom
```
RuntimeError: Failed client connect: ... tcp connect error, 159.69.143.122:7233, ... TimedOut
# and in `temporal` logs:
"error creating sdk client","service":"worker","error":"... context deadline exceeded ..."
# and `temporal worker list` fails with the same deadline-exceeded.
```

### Root cause — two distinct things
1. **Temporal wasn't running.** `docker compose ps` showed `temporal-ui` but no
   `infra-temporal-1` (it had died in an earlier disk-full window). With no
   running `temporal` service, Docker's embedded DNS has no record for it, so the
   resolver falls through to the **host search domain** and resolves
   `temporal.<search-domain>` to a **public** wildcard IP (`159.69.143.122`) —
   the confusing public-IP symptom. Once the container runs, `temporal` resolves
   to its internal `172.x` (service names beat search domains).
2. **Internal gRPC by IP got proxied.** Temporal's own system worker and the
   admin-tools CLI connect to the container **IP** (`172.18.0.6:7233`), not the
   hostname. `NO_PROXY` only listed service *names*, so those IP connections were
   routed through the corporate proxy → timed out. (The engine-worker connected
   via the *hostname* `temporal:7233`, which is in `NO_PROXY`, so it worked.)

### Fix
Start Temporal, and add the docker bridge subnet to `NO_PROXY` so IP-based
internal connections bypass the proxy:
```bash
sudo docker compose -f packages/infra/docker-compose.yml up -d temporal

# add 172.16.0.0/12 to noProxy in /root/.docker/config.json, then recreate:
#   "noProxy": "localhost,127.0.0.1,::1,172.16.0.0/12,postgres,seaweedfs,...,cockpit"
sudo docker compose -f packages/infra/docker-compose.yml up -d --force-recreate temporal engine-worker
```
Verify the worker is registered and the warnings stop:
```bash
sudo docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233      # -> Status: Running
```

---

## Problem 10 — Cockpit chat can't reach Anthropic (`ConnectionRefused`)

### Symptom
```
❌ [tanstack-ai:errors] ❌ anthropic.chatStream fatal
  error: Unable to connect. ... path: "https://api.anthropic.com/v1/messages?beta=true",
  code: "ConnectionRefused"
# plus repeated: SSR stream transform exceeded maximum lifetime (120000ms), forcing cleanup
```
(The `SSR stream ... lifetime` spam is just the chat SSE stream hanging 120s on
each failed Anthropic call — a symptom, not a separate bug.)

### Root cause
The cockpit runs with **no** global `HTTP_PROXY` (so `Bun.S3Client` stays direct,
Problem 6), which means the Anthropic SDK has no route to the internet unless the
proxy is applied explicitly. The `outbound-proxy.ts` shim is supposed to do that
for `*.anthropic.com` — but **the shim file was empty (0 bytes)**: the heredoc
that "created" it never wrote any content, so `config.ts`'s `import` pulled in an
empty module and no proxy was ever applied → direct connect → `ConnectionRefused`.

### Fix
Populate `packages/cockpit/src/outbound-proxy.ts` with the shim (see the
committed file), set `OUTBOUND_PROXY` in the override, and rebuild the cockpit so
the shim is compiled into `.output`. Verify it actually made it in:
```bash
wc -c packages/cockpit/src/outbound-proxy.ts                 # must be > 0 (~900 bytes)
sudo docker compose -f packages/infra/docker-compose.yml -f packages/infra/docker-compose.override.yml \
  up -d --build cockpit
sudo docker compose -f packages/infra/docker-compose.yml exec cockpit \
  sh -c 'grep -rl OUTBOUND_PROXY /app/.output/server | head'  # must find it
```

> **If the shim is present but chat still fails** (`OUTBOUND_PROXY` in the build,
> yet `ConnectionRefused`), the `@anthropic-ai/sdk` captured its `fetch` reference
> before the shim installed — the global-`fetch` patch lost the import-order race.
> The robust fallback is a **tinyproxy sidecar**: give the cockpit a normal
> `HTTP_PROXY=http://proxy:8888` pointing at a local proxy that forwards
> `*.anthropic.com` to the corporate proxy and connects internal hosts directly.
> That makes `Bun.S3Client` and the Anthropic SDK both work with no app code.

> **Build OOM note:** the cockpit `apt-get install wget` step can be `Killed`
> (exit 137) when the proxy is congested + RAM is tight. Retry; if it persists,
> switch the cockpit healthcheck off `wget` (it's only there because
> `oven/bun:1-slim` lacks it) so the apt step disappears.

---

## Problem 11 — Cockpit chat "stuck in connect" (DuckDB read-path extension download)

### Symptom
In the cockpit chat, the agent's `connect` / `run_sql` tool hangs on
`connecting…`, and the agent narrates something like "the DuckDB httpfs
extension isn't installed in this environment."

### Root cause
The cockpit's **read-path** DuckDB (`@duckdb/node-api`, used by
`src/duckdb/connect.ts`, `s3-secret.ts`, the lake reader) does `INSTALL httpfs`
(and `ducklake` / `postgres_scanner` for the lake) at runtime
([s3-secret.ts:62](../packages/cockpit/src/duckdb/s3-secret.ts#L62)). With the
cockpit running proxy-free (Problem 6) and DuckDB ignoring `HTTP_PROXY`, the
`INSTALL` download from `extensions.duckdb.org` stalls instead of failing fast —
so the tool sits on `connecting…`. The cockpit Dockerfile pre-bakes **no** DuckDB
extensions (unlike the engine worker, Problem 8).

### Fix (committed — runtime proxy stopgap)
`packages/cockpit/src/duckdb/proxy.ts` exports `applyDuckdbProxy(conn)`, which
does `SET http_proxy` from `OUTBOUND_PROXY` (bare host:port; no-op when unset).
It is called before every runtime `INSTALL` in the read path —
`s3-secret.ts` (httpfs), `lake.ts` (ducklake), `probe.ts` (DB scanners) — so the
extension downloads **through the proxy** instead of stalling. Cached after the
first cold-start fetch.

### Cleaner follow-up (not yet done — air-gapped)
The stopgap still hits the network on a cold start. To make it air-gapped like
the engine, **pre-bake the read-path extensions into the cockpit image**:
1. Cockpit `Dockerfile` build stage: run a bun script that `SET http_proxy` (from
   the build env), `SET extension_directory`, then `INSTALL httpfs ; ducklake ;
   postgres_scanner` into a known image path; `COPY` that dir into the runner.
2. Cockpit code: `SET extension_directory` (from an env var) on every DuckDB
   connection before `INSTALL`/`LOAD`, so the runtime read finds the cached
   extension and never touches the network.

---

## Accessing the cockpit from the network

The cockpit publishes `3000:3000` (bound to `0.0.0.0`), so it's reachable from
other machines once you have the host IP and an open firewall path.

```bash
# On the host: find the LAN IP
hostname -I

# Open the firewall if ufw is active
sudo ufw status
sudo ufw allow 3000/tcp
```
Then browse to `http://<host-ip>:3000`.

> **Proxy gotcha (client side):** if your browser/OS routes through the corporate
> proxy, a request to `http://<host-ip>:3000` may be sent to the proxy instead of
> the host. Add the host IP to your proxy **exceptions / no_proxy** list.

Test bypassing any proxy:
```bash
curl -sS -o /dev/null -w "%{http_code}\n" --noproxy '*' http://<host-ip>:3000   # expect 200
```

Other published ports: SeaweedFS S3 API on `8333`.

---

## Quickstart

For a fresh proxied Ubuntu host, in order:

```bash
# 1. Daemon DNS
sudo tee /etc/docker/daemon.json >/dev/null <<'EOF'
{ "dns": ["8.8.8.8", "1.1.1.1"] }
EOF
sudo systemctl restart docker

# 2. Client/build proxy (substitute your proxy URL)
sudo mkdir -p /root/.docker
sudo tee /root/.docker/config.json >/dev/null <<'EOF'
{
  "proxies": {
    "default": {
      "httpProxy": "http://10.2.1.198:8080",
      "httpsProxy": "http://10.2.1.198:8080",
      "noProxy": "localhost,127.0.0.1,::1,172.16.0.0/12,postgres,seaweedfs,seaweedfs-init,temporal,temporal-admin-tools,temporal-create-namespace,temporal-ui,engine-worker,cockpit"
    }
  }
}
EOF

# 3. Pre-pull base images through the daemon (avoids BuildKit manifest timeouts)
sudo docker pull oven/bun:1-slim
sudo docker pull oven/bun:1
sudo docker pull python:3.14-slim
sudo docker pull ghcr.io/astral-sh/uv:latest

# 4. Build + start
sudo docker compose -f packages/infra/docker-compose.yml up -d --build --wait

# 5. Verify
sudo docker compose -f packages/infra/docker-compose.yml ps
sudo docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233      # -> Status: Running
curl -sS -o /dev/null -w "%{http_code}\n" --noproxy '*' http://localhost:3000   # -> 200
```

```bash
# 6. PROXIED HOSTS ONLY — split the cockpit's traffic: S3/internal direct,
#    Anthropic via the proxy (Problem 6). Bun.S3Client ignores NO_PROXY, so a
#    global HTTP_PROXY would 503 the object-store calls.
cat > packages/infra/docker-compose.override.yml <<'EOF'
services:
  cockpit:
    environment:
      HTTP_PROXY: ""
      HTTPS_PROXY: ""
      http_proxy: ""
      https_proxy: ""
      OUTBOUND_PROXY: "http://10.2.1.198:8080"
EOF
# Recreate cockpit WITH the override (both -f flags — explicit -f kills auto-load):
sudo docker compose \
  -f packages/infra/docker-compose.yml \
  -f packages/infra/docker-compose.override.yml \
  up -d --no-deps cockpit
```

If a build runs out of disk: `sudo docker builder prune -af`, then re-run step 4
**without** `--build` (the images are already built). Never use
`docker image prune -a` (it removes the pre-pulled bases — Problem 2/7).

---

## Permanent fixes carried in the repo

These are committed and are no-ops without a proxy, so they're safe everywhere:

- `packages/engine/docker/worker.Dockerfile` — DuckDB `SET http_proxy` before
  `INSTALL` (Problem 3) **and** pre-bakes `postgres_scanner` (Problem 8).
- `packages/infra/docker-compose.yml` — SeaweedFS healthcheck clears proxy env
  (Problem 5).
- `packages/cockpit/src/outbound-proxy.ts` + the `import` in
  `packages/cockpit/src/config.ts` — proxy only `*.anthropic.com`; no-op unless
  `OUTBOUND_PROXY` is set (Problem 6/10).
- `packages/cockpit/src/duckdb/proxy.ts` (`applyDuckdbProxy`), called before
  `INSTALL` in `s3-secret.ts` / `lake.ts` / `probe.ts` — `SET http_proxy` from
  `OUTBOUND_PROXY` so read-path DuckDB extensions download through the proxy;
  no-op unless set (Problem 11).

The proxy/DNS host config (Problems 1, 2, 4) and the cockpit override
(`docker-compose.override.yml`, Problem 6) are **environment-specific** and are
**not** in the repo — they live in `/etc/docker/daemon.json`,
`/root/.docker/config.json`, the systemd drop-in, and the host-local override.

## Follow-ups / known sharp edges

- **Pin `chrislusf/seaweedfs`** to a specific tag instead of `:latest` — a
  `:latest` drift (a busybox/wget change) is what made Problem 5 surface.
- **`Bun.S3Client` ignores `NO_PROXY`** — this is why Problem 6 needs the
  per-request proxy shim rather than an env fix. Revisit if Bun adds `NO_PROXY`
  support (then the cockpit could use a normal `HTTP_PROXY` + `NO_PROXY`).
- **Never run `docker image prune -a`** here — it deletes the pre-pulled base
  images and reintroduces Problem 2. Use `docker builder prune -af`.
- **Pass both `-f` flags** for any command once `docker-compose.override.yml`
  exists — explicit `-f` disables its auto-loading (Problem 6).
- **`NO_PROXY` needs the docker subnet (`172.16.0.0/12`)**, not just service
  names — internal gRPC that connects by container IP (Temporal) is otherwise
  proxied (Problem 9).
- **A "created" file can be empty.** Problem 10 was a 0-byte `outbound-proxy.ts`
  from a heredoc that didn't write — always `wc -c` after a heredoc and confirm
  the change is in the build (`grep -rl … /app/.output`).
- **Cockpit read-path DuckDB extensions download at runtime** (Problem 11) — the
  committed `applyDuckdbProxy` stopgap fixes the proxy stall, but it's a cold-start
  network fetch. Pre-baking them into the cockpit image (mirror the engine worker)
  is the air-gapped close-out.
- **Grow the root disk** — 22 GB leaves almost no headroom for image rebuilds +
  data volumes.
