# 🛒 Architecture Big Data — Analyse des Ventes d'un Supermarché

Projet académique mettant en œuvre une architecture Big Data complète (ingestion & stockage) sous **Cloudera**, avec **Kafka**, **Sqoop** et **Flume**.

Réalisé par **ANANE Ines** & **BEN NACEUR Zeineb**

---

## 📌 Contexte & Problématique

Dans un supermarché, la gestion des ventes et des stocks nécessite l'analyse de volumes de données importants, variés et générés en continu — un défi difficile à relever avec des outils traditionnels.

**Problématique :** Comment exploiter efficacement les données de ventes d'un supermarché pour améliorer la gestion des stocks et anticiper les tendances, tout en gérant des volumes importants de données en temps réel ?

**Dataset utilisé :** [Supermarket Data (Kaggle)](https://www.kaggle.com/datasets/saurabhbadole/supermarket-data) — transactions, produits, clients, pays.

## 🏗️ Architecture de la solution

```
Sources de données (CSV, logs, MySQL)
        ↓
Kafka (ingestion temps réel) ──┐
Sqoop (transfert MySQL)  ──────┼──► HDFS (stockage distribué)
Flume (collecte de logs) ──────┘
```

---

## ⚙️ Environnement — Cloudera

Mise en place de l'environnement de travail via la VM Cloudera QuickStart.

| Étape | Capture |
|---|---|
| Téléchargement de la VM Cloudera | ![cloudera-telechargement](screenshots/cloudera/cloudera-01-telechargement.png) |
| Importation dans VirtualBox | ![cloudera-importation](screenshots/cloudera/cloudera-02-importation.png) |
| Interface Cloudera | ![cloudera-interface](screenshots/cloudera/cloudera-03-interface.png) |
| Vérification de l'utilisateur connecté (`whoami`) | ![cloudera-whoami](screenshots/cloudera/cloudera-04-whoami.png) |

---

## 📨 Kafka — Ingestion en temps réel

**Rôle :** ingérer les données de ventes en temps réel via un système de topics (producer/consumer).

| Étape | Explication | Capture |
|---|---|---|
| 1. Vérification | Vérification que Kafka n'est pas déjà installé (`ls /usr/lib \| grep kafka`) | ![kafka-verif](screenshots/kafka/kafka-01-verification.png) |
| 2. Installation | Téléchargement de Kafka 0.8.2.2 | ![kafka-install](screenshots/kafka/kafka-02-installation.png) |
| 3. Résolution erreur SSL | Contournement de l'erreur de certificat SSL via le navigateur | ![kafka-ssl](screenshots/kafka/kafka-03-solution-ssl.png) |
| 4. Exception de sécurité | Confirmation de l'exception de sécurité pour le téléchargement | ![kafka-exception](screenshots/kafka/kafka-04-security-exception.png) |
| 5. Vérification du téléchargement | Contrôle du fichier `.tgz` téléchargé | ![kafka-verif-dl](screenshots/kafka/kafka-05-verif-telechargement.png) |
| 6. Extraction | Décompression de l'archive Kafka | ![kafka-extraction](screenshots/kafka/kafka-06-extraction.png) |
| 7. Démarrage Zookeeper | Zookeeper coordonne les métadonnées et les brokers Kafka | ![kafka-zk-start](screenshots/kafka/kafka-07-zookeeper-start.png) |
| 8. Commande Zookeeper | `bin/zookeeper-server-start.sh config/zookeeper.properties` | ![kafka-zk-cmd](screenshots/kafka/kafka-08-zookeeper-commande.png) |
| 9. Cloudera Manager | Lancement du Cloudera Manager pour gérer les services | ![kafka-cm](screenshots/kafka/kafka-09-cloudera-manager.png) |
| 10. Connexion | Authentification (cloudera/cloudera) | ![kafka-login](screenshots/kafka/kafka-10-login.png) |
| 11. Démarrage Kafka | `bin/kafka-server-start.sh config/server.properties` | ![kafka-server](screenshots/kafka/kafka-11-server-start.png) |
| 12. Création d'un topic | Création et listing du topic `my-topic` | ![kafka-topic](screenshots/kafka/kafka-12-creation-topic.png) |
| 13. Producer | Envoi de messages (transactions) dans le topic | ![kafka-producer](screenshots/kafka/kafka-13-producer.png) |
| 14. Consumer | Lecture des messages depuis le topic | ![kafka-consumer](screenshots/kafka/kafka-14-consumer.png) |
| 15. Test 1 | Vérification du flux producer → consumer | ![kafka-test1](screenshots/kafka/kafka-15-test1.png) |
| 16. Test 2 | Test complémentaire du pipeline Kafka | ![kafka-test2](screenshots/kafka/kafka-16-test2.png) |

---

## 🔄 Sqoop — Transfert MySQL → HDFS

**Rôle :** transférer les données structurées depuis MySQL vers HDFS.

| Étape | Explication | Capture |
|---|---|---|
| 1. Vérification version | `sqoop version` (1.4.6-cdh5.12.0) | ![sqoop-version](screenshots/sqoop/sqoop-01-version.png) |
| 2. État HDFS | `hdfs dfsadmin -report` — vérifie NameNode, DataNodes, espace disponible | ![sqoop-report](screenshots/sqoop/sqoop-02-hdfs-report.png) |
| 3. Désactivation Safe Mode | Sortie du mode sécurisé HDFS | ![sqoop-safemode](screenshots/sqoop/sqoop-03-safemode.png) |
| 4. Nouveau test | Re-vérification de l'état HDFS après correction | ![sqoop-retest](screenshots/sqoop/sqoop-04-retest.png) |
| 5. Import CSV → MySQL | Importation du fichier CSV dans la table `supermarket_data` | ![sqoop-import-csv](screenshots/sqoop/sqoop-05-import-csv.png) |
| 6. LOAD DATA | Requête SQL de chargement des données | ![sqoop-load](screenshots/sqoop/sqoop-06-load-data.png) |
| 7. Démarrage NameNode | Service HDFS NameNode | ![sqoop-namenode](screenshots/sqoop/sqoop-07-namenode-start.png) |
| 8. Démarrage DataNode | Service HDFS DataNode + YARN ResourceManager | ![sqoop-datanode](screenshots/sqoop/sqoop-08-datanode-start.png) |
| 9. Services YARN | NodeManager + HistoryServer | ![sqoop-yarn](screenshots/sqoop/sqoop-09-yarn-start.png) |
| 10. Vérification HDFS | Lecture des données importées dans HDFS | ![sqoop-verif-hdfs](screenshots/sqoop/sqoop-10-verification-hdfs.png) |
| 11. Import final | Exécution de la commande `sqoop import` (MySQL → HDFS) | ![sqoop-import-final](screenshots/sqoop/sqoop-11-import-final.png) |

**Commande clé :**
```bash
sqoop import --connect jdbc:mysql://localhost/supermarket_data \
  --username root --password cloudera \
  --table supermarket_sales \
  --target-dir /user/root/supermarket_data \
  --m 1 --direct
```

---

## 🌊 Flume — Collecte de logs

**Rôle :** collecter et traiter des fichiers logs.

| Étape | Explication | Capture |
|---|---|---|
| 1. Vérification version | `flume-ng version` (1.6.0-cdh5.12.0) | ![flume-version](screenshots/flume/flume-01-version.png) |
| 2. Téléchargement | Téléchargement de Flume 1.9.0 | ![flume-dl](screenshots/flume/flume-02-telechargement.png) |
| 3. Extraction | Décompression de l'archive | ![flume-extraction](screenshots/flume/flume-03-extraction.png) |
| 4. Conversion CSV → log | Script Python convertissant les données en fichier `.log` | ![flume-conversion](screenshots/flume/flume-04-conversion-csv.png) |
| 5. Configuration de l'agent | Définition source / channel / sink dans `flume.conf` | ![flume-config](screenshots/flume/flume-05-configuration.png) |

**Configuration de l'agent Flume :**
```properties
agent.sources = source1
agent.sinks = sink1
agent.channels = channel1

agent.sources.source1.type = exec
agent.sources.source1.command = tail -F /var/log/messages

agent.channels.channel1.type = memory
agent.channels.channel1.capacity = 1000
agent.channels.channel1.transactionCapacity = 100

agent.sinks.sink1.type = file_roll
agent.sinks.sink1.sink.directory = /home/cloudera/flume-logs
agent.sinks.sink1.sink.rollInterval = 60
```

---

## 🛠️ Stack technique

**Ingestion :** Kafka, Flume
**Transfert :** Sqoop
**Stockage :** HDFS (Hadoop)
**Base de données :** MySQL
**Environnement :** Cloudera QuickStart VM, VirtualBox

## 🎯 Conclusion

Ce projet démontre la mise en place d'une architecture Big Data complète pour la collecte et le stockage distribué de données de ventes — de l'ingestion en temps réel (Kafka) au transfert structuré (Sqoop) et à la collecte de logs (Flume), le tout centralisé dans HDFS pour un traitement ultérieur.
