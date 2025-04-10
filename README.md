# Salut 👋

## Table des matières
1. [Présentation](#Présentation)
2. [Applications](#applications)
3. [Exécution d'un cluster Big Data](#Exécution d'un cluster Big Data)
4. [Accès aux conteneurs Docker](#access-docker-containers)
5. [Accès à l'écosystème Hadoop](#access-hadoop-ecosystem)
6. [Conteneurs exécutés dans le cluster](#containers-running-in-the-cluster)
7. [Accès à HDFS](#access-hdfs)
8. [Accès à la base de données Postgres](#access-postgres-db)
9. [Accès à Kadmin](#access-kadmin)
10. [Exécution de requêtes Hive](#run-hive-queries)
11. [Exécution de PySpark](#run-pyspark)
12. [Exécution de sqoop](#run-sqoop)
13. [Exécution d'airflow](#run-airflow)
14. [Création d'une rubrique Kafka](#create-kafka-topic)
15. [Créer un producteur Kafka](#create-kafka-producer)
16. [Créer un consommateur Kafka](#create-kafka-consumer)
17. [Exécuter les agents Flume](#run-flume-agents)
18. [Exécuter les requêtes dans Cassandra](#run-queries-in-cassandra)
19. [Jars disponibles](#available-jars)
20. [Configurer Hadoop](#configure-hadoop)
21. [Configurer Hue](#configure-hue)
22. [Configurer Zeppelin](#configure-zeppelin)
23. [Configurer Airflow](#configure-airflow)
24. [Configurer Sqoop](#configure-sqoop)
25. [Configurer le serveur SSH](#configure-ssh-server)
26. [Créer un Docker personnalisé Images](#build-custom-docker-images)
27. [Gérer les ressources](#manage-resources)
28. [Dépendances](#dependencies)

## Présentation
Aujourd'hui, de nombreux projets ont été créés pour déployer un cluster Spark ou Hadoop, mais ils sont soit inefficaces, soit gourmands en ressources, ce qui entraîne des blocages du système.
Ce dépôt a été créé pour simplifier le déploiement de ces clusters sur une machine locale à l'aide de conteneurs Docker.

## Applications
Ce projet démarrera un cluster Docker donnant accès aux frameworks/technologies suivants.
|Framework/Technology | Version |
|--- |--- |
| Hadoop | 3.2.1 |
| Mini-Conda | 4.12.0 |
| Python | 3.9 |
| Spark | 3.2.2 (créé avec Scala 2.12) |
| Airflow | 2.3.3 |
| Zeppelin | 0.10.1 |
| Hive | 2.3.2 |
| Hue | 4.6.0 |
| Sqoop | 1.4.7 |
| Confluent Kafka | 5.4.0-ce |
| Confluent Schema Registry | 5.4.0-ce |
| Confluent Control Center | 5.4.0 |
| Kadmin | 0.9.3 |
| Flume | 1.11.0 |
| Postgres | 15.1 |
| pgAdmin4 | 6.18 |
| Cassandra | 4.1.0 |

## Exécution du cluster Big Data
Pour démarrer le cluster, exécutez la commande suivante depuis le répertoire du projet.

##### Sous Linux : clonage du projet : 
```sh
cd ~
rm -Rf Big-Data-Cluster
git clone https://github.com/crystalloide/Big-Data-Cluster
cd Big-Data-Cluster
```

### Démarrer le cluster de base
Le cluster de base donne accès à Hadoop, PySpark, Airflow, Flume et Zeppelin. C'est un excellent point de départ si vous souhaitez tester certains de ces outils sans avoir à les installer sur votre ordinateur.

##### Sous Linux
```sh
sudo docker compose -f basic-hadoop-docker-compose.yaml up
```

###### Sous Windows 10
```sh
docker compose -f basic-hadoop-docker-compose.yaml up
```
> Remarque : Vous devez installer WSL 2 sous Windows pour exécuter ce cluster.

### Démarrer le cluster de base + Hive + Hue + DB
Ce fichier démarre le cluster de base, Hive, Hue, Postgres, pgAdmin et Cassandra. Il est idéal pour créer des pipelines de traitement par lots.

##### Sous Linux
```sh
sudo docker compose -f hive-sqoop-postgres-cassandra-docker-compose.yaml up
```

##### Sous Windows 10
```sh
docker compose -f hive-sqoop-postgres-cassandra-docker-compose.yaml up
```
> Remarque : Vous devez installer WSL 2 sous Windows pour exécuter ce cluster.

### Démarrer le cluster de base + Kafka + registre de schéma
Ce fichier démarre le cluster de base, ZooKeeper, le broker Kafka et le registre de schéma. Il est idéal pour créer des pipelines de streaming.

##### Sous Linux
```sh
sudo docker compose -f kafka-docker-compose.yaml up
```

##### Sous Windows 10
```sh
docker compose -f kafka-docker-compose.yaml up
```
> Remarque : Vous devez installer WSL 2 sous Windows pour exécuter ce cluster.

### Démarrer le cluster Kafka
Ce fichier démarre un cluster Kafka. Il est idéal pour démarrer avec Kafka et le registre de schémas.

##### Sous Linux
```sh
sudo docker compose -f kafka-zookeper.yaml up
```

##### Sous Windows 10
```sh
docker compose -f kafka-zookeper.yaml up
```
> Remarque : Vous devez installer WSL 2 sous Windows pour exécuter ce cluster.

### Démarrer le cluster complet
Ce fichier démarre tous les frameworks/technologies mentionnés [ici](#applications).

##### Sous Linux
```sh
sudo docker compose -f all-docker-compose.yaml up
```

###### Sous Windows 10
```sh
docker compose -f all-docker-compose.yaml up
```
> Remarque : Vous devez installer WSL 2 sous Windows pour exécuter ce cluster.

## Accéder aux conteneurs Docker
Vous pouvez accéder à un conteneur
