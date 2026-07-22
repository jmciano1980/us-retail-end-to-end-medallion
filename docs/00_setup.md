# Setup - WSL2 + Native Docker Engine + Postgres

This project keeps Host clean. All infra runs in WSL2.

#### Host Requirements
- Windows 11 22H2+, 64GB RAM, Virtualization enabled in BIOS
- WSL2 with systemd enabled

#### 1. Enable WSL2
Powershell as Admin:
wsl --install -d Ubuntu-24.04
wsl --update

Configure C:\Users\<You>\.wslconfig:
[wsl2]
memory=16GB
processors=6

#### 2. Install Docker Engine inside Ubuntu (no Docker Desktop)
Inside Ubuntu-24.04 terminal:
- Enable systemd via /etc/wsl.conf -> [boot] systemd=true
- Install docker-ce, docker-compose-plugin from Docker official repo
- sudo usermod -aG docker $USER

Verify: docker run hello-world

#### 3. Start Postgres
cd ~/projects/us-retail-end-to-end-medallion
docker compose up -d
docker ps -> retail_postgres healthy

Connection from WSL2: localhost:5432
Connection from VMWare Dev VM: hostname -I -> 172.x.x.x:5432
Firewall rule on Host: New-NetFirewallRule for 5432 TCP

#### Troubleshooting
- init.sql runs only on first volume creation. To rerun: docker compose down -v
- Port conflict: Host Postgres uses 5432. Change to 5433:5432 in compose or stop Host service.
