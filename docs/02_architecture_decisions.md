# Architecture Decision Records (ADR)

### ADR-001: Use Native Docker Engine inside WSL2, no Docker Desktop
Date: 2026-07-22
Context: Host must stay clean, only WSL2 + VMWare.
Decision: Install docker-ce directly in Ubuntu-24.04 with systemd=true. No Docker Desktop on Host.
Consequence: More production-like, less RAM, but no GUI. Must use docker ps via CLI.

### ADR-002: Make retail_raw permissive for error injection
Date: 2026-07-22
Context: Initial init.sql had PK, FK, CHECK that prevented loading 1-2% dirty data.
Decision: Remove all business constraints in retail_raw. Keep only raw_id BIGSERIAL PK. Change most columns to VARCHAR nullable.
Consequence: Postgres will accept nulls, duplicates, invalid refs, invalid formats. All quality checks move to Databricks Silver layer where we can quarantine and log errors. This correctly models Bronze -> Silver.
