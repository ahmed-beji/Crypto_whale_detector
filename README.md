# 🐋 Crypto Whale Tracker - Real-Time ETL Pipeline

[![CI/CD Pipeline](https://github.com/ahmed-beji/Detection_fraude/actions/workflows/deploy.yml/badge.svg)](https://github.com/ahmed-beji/Detection_fraude/actions)
[![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![Infrastructure](https://img.shields.io/badge/IaC-Terraform-7B42BC.svg)](https://terraform.io)

Ce projet est un pipeline de données (ETL) robuste, asynchrone et conteneurisé. Il est conçu pour ingérer un flux continu de transactions en temps réel depuis l'API WebSocket de Binance, détecter les mouvements institutionnels majeurs ("Baleines" ou Smart Money), et stocker ces données pour une visualisation instantanée.

L'infrastructure est provisionnée dynamiquement sur AWS via Terraform, et le déploiement est automatisé par un pipeline CI/CD GitHub Actions.

---

## 📊 Dashboard & Métriques en Production

> **Note :** *[Insérer ici une capture d'écran de votre dashboard Metabase avec quelques graphiques de volume et de répartition]*
> `![Metabase Dashboard](./assets/dashboard_preview.png)`

**Performances du Pipeline :**
* **Débit d'ingestion :** ~ `[X]` messages traités par seconde.
* **Latence (Bout-en-bout) :** ~ `[Y]` millisecondes entre l'ordre Binance et l'insertion PostgreSQL.
* **Volume identifié :** `[Z]` transactions exceptionnelles détectées sur 7 jours.
* **Coût Infrastructure :** ~ `[Prix]` $ / mois (Architecture optimisée sur une seule instance EC2 `t2.micro` / `t3.micro`).

---

## 🏗️ Architecture du Système

L'architecture abandonne les requêtes HTTP synchrones classiques au profit d'une connexion WebSocket pour garantir un véritable temps réel, avec une gestion stricte de la résilience logicielle.

1. **Extract (WebSockets) :** Connexion asynchrone au flux public de Binance (Paire BTC/USDT) avec **stratégie de reconnexion automatique (Exponential Backoff)** pour pallier les coupures serveurs de 24h.
2. **Transform (Buffer in-memory) :** Nettoyage à la volée des payloads JSON. Les données sont conservées en RAM (flush toutes les `[X]` secondes ou `[Y]` messages) pour limiter les I/O.
3. **Load (Hot Storage - PostgreSQL) :** Insertion des transactions majeures pour la visualisation. La contrainte d'unicité sur le `trade_id` (`ON CONFLICT DO NOTHING`) garantit l'absence de doublons lors des reconnexions au réseau.
4. **Load (Cold Storage - S3) :** Purge régulière du buffer RAM, compression au format **Parquet** via PyArrow, et envoi vers un Data Lake AWS S3 avec partitionnement par date (`dt=YYYY-MM-DD`).
5. **Dataviz (Metabase) :** Tableau de bord branché sur PostgreSQL.
6. **Infrastructure (AWS) :** Déploiement automatisé sur EC2.

```mermaid
flowchart LR
    subgraph AWS [Infrastructure AWS]
        direction TB
        subgraph Docker [Stack Docker Compose EC2]
            A[Moteur Ingestion Python<br/>WebSocket Consumer]
            B[(PostgreSQL<br/>Hot Storage)]
            D[Metabase<br/>Dashboard Dataviz]
        end
        C[(AWS S3<br/>Data Lake Parquet)]
    end

    API((API Binance)) == Flux BTC/USDT<br/>Temps Réel ==> A
    A -- Filtre Volume<br/>Écriture instantanée --> B
    A -- Flush RAM (PyArrow)<br/>Partitionnement dt=YYYY-MM-DD --> C
    B -. Requêtes SQL .-> D
    User((Data Engineer)) == Port 3000<br/>HTTP ==> D

    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:black;
    classDef docker fill:#2496ED,stroke:#fff,stroke-width:2px,color:white;
    class AWS aws;
    class Docker docker;