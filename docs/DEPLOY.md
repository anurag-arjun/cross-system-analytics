# Deploying to shieldtx-vps (BD MVP)

Target: `analytics.themuse.one` on `shieldtx-vps` (`apnetv@149.102.139.222`,
Ubuntu 24.04).

The host already runs other services (devpush stack, shieldtx-{ch,pg,api},
arr/seedbox). Edge ingress is via **Cloudflare Tunnel** because public 80/443
are not free.

## Topology

```
            ┌────────────────── Cloudflare ─────────────────┐
            │  TLS termination · Access (email allowlist)   │
            └────────────┬──────────────────────────────────┘
                         │ cloudflared tunnel
                         ▼
            127.0.0.1:18080  ── nexus-nginx  ── /api/* ─►  nexus-api  (uvicorn:8000)
                                              ── /     ─►  /var/www/html  (frontend/dist)

  nexus-ch  ─ 127.0.0.1:18123 / :19000   (ClickHouse 24.3, db `nexus`)
  nexus-pg  ─ 127.0.0.1:15434           (Postgres 16, db `nexus_ops`)
```

Cron + backfill run on the host (system Python venv at
`/home/apnetv/nexus-analytics/.venv`), not in containers.

## Steps

1. **Deploy key + clone**
   ```bash
   ssh shieldtx-vps 'ssh-keygen -t ed25519 -N "" -f ~/.ssh/nexus_deploy -C nexus@shieldtx-vps && cat ~/.ssh/nexus_deploy.pub'
   # Add the printed pubkey to GitHub repo Settings -> Deploy keys (read-only).
   ssh shieldtx-vps 'echo -e "Host github.com-nexus\n  Hostname github.com\n  IdentityFile ~/.ssh/nexus_deploy\n  IdentitiesOnly yes" >> ~/.ssh/config'
   ssh shieldtx-vps 'git clone git@github.com-nexus:anurag-arjun/cross-system-analytics.git ~/nexus-analytics'
   ```

2. **Secrets**
   ```bash
   # Paste keys into a heredoc-built .env on the VPS (chmod 600).
   ssh shieldtx-vps 'cat > ~/nexus-analytics/.env && chmod 600 ~/nexus-analytics/.env' <<EOF
   HYPERSYNC_TOKEN=...
   ETHERSCAN_API_KEY=...
   DUNE_API_KEY=...
   COINGECKO_API_KEY=...
   CLICKHOUSE_HOST=localhost
   CLICKHOUSE_PORT=18123
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=nexus
   CLICKHOUSE_DB=nexus
   PROTOCOL_CONTRACTS_DSN=postgresql://nexus:nexus@localhost:15434/nexus_ops
   EOF
   ```

3. **Bring up the data + serving stack**
   ```bash
   ssh shieldtx-vps 'cd ~/nexus-analytics && docker compose -f docker-compose.prod.yml up -d ch pg'
   # Wait for healthchecks, then apply Postgres schemas:
   ssh shieldtx-vps 'cd ~/nexus-analytics && for f in core/schemas/postgres/*.sql; do
       docker exec -i nexus-pg psql -U nexus -d nexus_ops < "$f" || true;
   done'
   ```

4. **Build the frontend on the VPS, then start nginx + api**
   ```bash
   ssh shieldtx-vps 'cd ~/nexus-analytics/frontend && nvm use --lts && npm ci && npm run build'
   ssh shieldtx-vps 'cd ~/nexus-analytics && docker compose -f docker-compose.prod.yml up -d --build api nginx'
   ```

5. **Cloudflare Tunnel**
   ```bash
   # Install cloudflared from Cloudflare's apt repo
   ssh shieldtx-vps 'sudo mkdir -p --mode=0755 /usr/share/keyrings &&
       curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | sudo tee /usr/share/keyrings/cloudflare-main.gpg >/dev/null &&
       echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/cloudflared.list &&
       sudo apt-get update && sudo apt-get install -y cloudflared'

   # Browser auth (interactive on user's laptop):
   ssh -t shieldtx-vps 'cloudflared tunnel login'
   ssh shieldtx-vps 'cloudflared tunnel create nexus-analytics'
   # Write config that routes analytics.themuse.one -> nexus-nginx
   # (see ops/cloudflared/config.yml)
   ssh shieldtx-vps 'cloudflared tunnel route dns nexus-analytics analytics.themuse.one'
   ssh shieldtx-vps 'sudo cloudflared service install $(cat ~/.cloudflared/<TUNNEL>.json | jq -r .TunnelSecret)'
   ```

   Then in Cloudflare dashboard → Zero Trust → Access → Applications, create
   a self-hosted app for `analytics.themuse.one` with an email-allowlist
   policy (the chosen access gate).

6. **Cron + 30-day backfill**
   ```bash
   ssh shieldtx-vps 'cd ~/nexus-analytics && python3 -m venv .venv &&
       .venv/bin/pip install -e core'
   # Install the cron entry from ops/CRON.md (path = ~/nexus-analytics, python = .venv/bin/python).
   ssh shieldtx-vps 'tmux new -d -s backfill "cd ~/nexus-analytics && PYTHONPATH=. .venv/bin/python ops/run_backfill.py --days 30 |& tee /tmp/backfill.log"'
   ```

7. **Verify**
   ```bash
   curl -s https://analytics.themuse.one/healthz
   curl -s 'https://analytics.themuse.one/api/bridge-flow/summary?days=7' | jq .
   ```

## Re-deploy after a code change

```bash
ssh shieldtx-vps '
  cd ~/nexus-analytics &&
  git pull &&
  cd frontend && npm ci && npm run build && cd .. &&
  docker compose -f docker-compose.prod.yml up -d --build api nginx
'
```
