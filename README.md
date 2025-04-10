# Salut 👋

## Table des matières
1. [Présentation](#Présentation)
2. [Applications](#applications)
3. [Exécution cluster Big Data](#Exécution_cluster_Big_Data)
4. [Accès aux conteneurs Docker](#Accès_aux_conteneurs_Docker)
5. [Accès à l'écosystème Hadoop](#Accès_écosystème_hadoop)
6. [Conteneurs exécutés dans le cluster](#containers_en_exécution_dans_le_cluster)
7. [Accès à HDFS](#Accès_à_HDFS)
8. [Accès à la base de données PostgreSQL](#Accès_à_la_base_de_données_PostgreSQL)
9. [Accès à Kadmin](#Accès_à_kadmin)
10. [Exécution de requêtes Hive](#Exécution_de_requêtes_Hive)
11. [Exécution de PySpark](#Exécution_de_PySpark)
12. [Exécution de Sqoop](#Exécution_de_Sqoop)
13. [Exécution d_Airflow](#Exécution d_Airflow)
14. [Création d'un topic Kafka](#Création_topic_kafka)
15. [Producteur Kafka](#Création_producer_kafka)
16. [Consommateur Kafka](#Création_consumer_kafka)
17. [Lancement_agents_Flume](#Lancement_agents_Flume)
18. [Exécution_requêtes_Cassandra](#Exécution_requêtes_Cassandra)
19. [Jars_disponibles](#Jars_disponibles)
20. [Configuration_Hadoop](#configuration-hadoop)
21. [Configuration_Hue](#configuration-hue)
22. [Configuration_Zeppelin](#configuration-zeppelin)
23. [Configuration_Airflow](#configuration-airflow)
24. [Configuration_Sqoop](#configuration-sqoop)
25. [Configuration_serveur_SSH](#configuration-ssh-server)
26. [Création_image_docker_personnalisée](#Création_image_docker_personnalisée)
27. [Gestion_des_ressources](#Gestion_des_ressources)
28. [Dépendances](#Dépendances)

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

________________________________________________________________________________________________
## Exécution_cluster_Big_Data
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

### Lancement_du_cluster_Kafka
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

________________________________________________________________________________________________
## Accès_aux_conteneurs_Docker
Vous pouvez accéder à un conteneur ainsi : 
```sh
sudo docker exec -it <nom-conteneur> /bin/bash
```
Remarque : Dans certains cas où cela ne fonctionne pas, remplacez `/bin/bash` par `/bin/sh`

________________________________________________________________________________________________
## Accès_écosystème_hadoop
Voici la liste des technologies ou frameworks exposés à l'hôte. Vous pouvez y accéder via les URL suivantes :
|Application | URL |
|--- |--- |
| Interface utilisateur Namenode | http://localhost:9870/dfshealth.html#tab-overview |
| Namenode (port IPC) | http://localhost:9000 |
| Serveur d'historique | http://localhost:8188/applicationhistory |
| Datanode | http://localhost:9864/ |
| Gestionnaire de nœuds | http://localhost:8042/node |
| Gestionnaire de ressources | http://localhost:8088/ |
| Hue | http://localhost:8888 |
| Interface utilisateur Spark Master | http://localhost:8080 |
| Interface utilisateur Spark Slave | http://localhost:8081 |
| Interface utilisateur Spark Driver | http://localhost:4040 (accessible uniquement après le démarrage d'un pilote) |
| Interface utilisateur Zeppelin | http://localhost:8082 |
| Interface utilisateur Airflow | http://localhost:3000 |
| Interface utilisateur pgAdmin | http://localhost:5000 |
| Zookeeper | http://localhost:2181 |
| Broker Kafka | http://localhost:9092 |
| Registre de schémas | http://localhost:8083 |
| Interface utilisateur Kadmin | http://localhost:8084/kadmin/ |
| Centre de contrôle Kafka | http://localhost:9021 |

________________________________________________________________________________________________
## containers_en_exécution_dans_le_cluster
Voici la liste des conteneurs exécutés dans le cluster.
|Nom du conteneur | Frameworks/Technologies installés | Description |
|--- |--- |--- |
| namenode | Namenode HDFS, Spark Master Node, Spark Slave Node, Zeppeline, Airflow, Flume et Python | Il agit comme un namenode et a également accès à d'autres frameworks/technologies. |
| datanode | Datanode HDFS, Python | Il agit comme un datanode et Python est installé. |
| resourcemanager | YARN, Python | Il agit comme YARN et Python est installé. Le planificateur YARN par défaut est défini sur « CapacityScheduler ». |
| nodemanager | Nodemanager, Python | Il agit comme un nodemanager et Python est installé. |
| historyserver | Serveur d'historique | Le suivi des tâches passées est délégué au serveur d'historique. |
| hive-server | Serveur Hive 2, Sqoop | Le serveur Hive 2 s'exécute sur ce conteneur. Vous pouvez également créer et exécuter des tâches Sqoop depuis ce conteneur. |
| hive-metastore | Métastore Hive | Le service de métastore Hive s'exécute ici. Il est utilisé pour stocker les métadonnées liées à Hive. |
| hive-metastore-postgresql | Postgres | Utilisé par le métastore Hive pour stocker les métadonnées. |
| hue | Serveur Hue | Serveur Hue exécuté ici. |
| huedb | Postgres | Utilisé par le serveur Hue pour stocker les métadonnées. |
| external_postgres_db | Postgres | Il s'agit d'une base de données relationnelle où vous pouvez stocker vos données. |
| external_pgadmin | pgAdmin | Vous pouvez l'utiliser pour accéder au serveur Postgres exécuté dans le conteneur external_postgres_db. |
| cassandra | Cassandra | Il s'agit d'une base de données non relationnelle orientée colonnes où vous pouvez stocker vos données. |
| zookeeper | Confluent Zookeper | Ce conteneur exécute ZooKeeper, qui assure le suivi du broker Kafka. |
| kafka-broker | Broker Kafka Confluent | Ce conteneur exécute un broker Kafka Confluent. |
| schema-registry | Registre de schémas Confluent | Ce conteneur exécute le registre de schémas Confluent, que vous pouvez utiliser pour stocker des schémas. |
| kadmin | Kadmin | Ce conteneur exécute kadmin. Vous pouvez utiliser cette application pour produire et consommer des messages. Elle prend en charge les données au format AVRO. |
| control-center | Centre de contrôle Confluent | Ce conteneur exécute le centre de contrôle Confluent. Ce centre de contrôle permet de consommer des messages, de consulter l'état du broker et de créer des schémas dans schema-registry. |

________________________________________________________________________________________________
## Accès_à_HDFS
Vous pouvez accéder à HDFS depuis l'interface utilisateur Hue ou Namenode.
> Remarque : Hue vous permet d'accéder au contenu des fichiers sur HDFS, mais l'interface utilisateur de Namenode ne le prend pas en charge.

Vous pouvez également accéder à HDFS depuis les conteneurs exécutant Hadoop, tels que Namenode.

________________________________________________________________________________________________
## Accès_à_la_base_de_données_PostgreSQL
Vous pouvez accéder au serveur PostgreSQL via pgAdmin. Vous pouvez également exécuter des requêtes SQL.

##### Identifiants Postgres
- utilisateur : external
- mot de passe : external
- base de données : external

##### Identifiants pgAdmin
- e-mail : pgadmin@xyz.com
- mot de passe : external

________________________________________________________________________________________________
## Accès_à_kadmin
Pour y accéder, consultez la section [Accès à l'écosystème Hadoop](#access-hadoop-ecosystem). Vous pouvez créer des producteurs et des consommateurs sur Kadmin. L'hôte Kafka et l'URL du registre de schémas Kafka vous seront demandés avant de créer un producteur ou un consommateur. Vous pouvez définir l'hôte Kafka et l'URL du registre de schémas Kafka sur « kafka-broker:29092 » et « http://schema-registry:8083 » respectivement.

________________________________________________________________________________________________
## Exécution_de_requêtes_Hive
Vous pouvez exécuter des requêtes Hive avec Hue.
Un serveur Thrift est exécuté sur Hiveserver 2. Hue a été configuré pour établir une connexion avec le serveur Thrift, qui exécute vos requêtes Hive.

________________________________________________________________________________________________
## Exécution_de_PySpark
Spark est installé dans « namenode ». Vous devez accéder au conteneur Namenode et exécuter la commande « spark-submit » pour exécuter les tâches Spark.

Dans ce cluster, vous pouvez exécuter Spark selon trois modes :
|Mode | Commande |
|--- |--- |
| Local | spark-submit [chemin_script_python] |
| [YARN](https://spark.apache.org/docs/latest/running-on-yarn.html) | spark-submit --master yarn --deploy-mode cluster --archives [chemin_fichier] --queue default [chemin_script_python] |
| [Autonome](https://spark.apache.org/docs/latest/spark-standalone.html) | spark-submit --master spark://namenode:7077 --deploy-mode cluster [chemin_script_python] |

##### Exécution_de_PySpark_en_mode_YARN
Dans ce mode, YARN planifie les tâches Spark. Vous devez créer un fichier compressé contenant tous les packages si vous utilisez des packages externes, tels que pandas.

Exécutez la commande ci-dessous pour créer ce fichier compressé
```sh
conda pack -f -o conda_env.tar.gz
```

La commande ci-dessus crée le fichier `conda_env.tar.gz` dans le répertoire courant. Vous pouvez ensuite passer le chemin absolu du fichier dans l'option `--archives`.

> REMARQUE : Vous pouvez choisir de ne pas ajouter l'option `--archives` si vous n'utilisez pas de paquets externes.

##### Exécuter PySpark dans Zeppeline
À des fins de développement, vous pouvez utiliser [Zeppelin](https://zeppelin.apache.org/docs/latest/interpreter/spark.html) pour exécuter des tâches Spark. Sélectionnez l'interpréteur Spark lors de la création d'un script dans Zeppelin. N'oubliez pas d'ajouter `%spark.pyspark` au début du bloc/de la cellule. Conservez votre code Spark dans un seul bloc/une seule cellule.
> Remarque : Zeppeline est configuré pour exécuter des tâches Spark en mode local.

##### Planifier des tâches PySpark dans Airflow
Vous pouvez planifier des tâches Spark depuis [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/). Vous avez besoin du [fournisseur Spark](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html) déjà installé pour planifier des tâches Spark. Pour exécuter des tâches Spark dans un cluster Spark autonome, définissez l'hôte sur « namenode » et le port sur « 7077 » lors de la création d'une connexion Spark dans Airflow.

________________________________________________________________________________________________
## Exécution_de_Sqoop
[Sqoop](https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html) est installé dans le conteneur Hive-server. Vous trouverez ci-dessous un exemple de commande pour importer des données avec sqoop. ```sh
sqoop import --connect jdbc:postgresql://external_postgres_db/external --username external --password external --table <nom-de-votre-table> --target-dir <répertoire-dans-hdfs> --m 1
```
Vous pouvez configurer le mappeur selon vos besoins. Étant donné que nous sommes en développement, je suggère d'en utiliser un seul.

Sqoop est configuré pour stocker le mot de passe de la base de données lors de la création de la tâche sqoop. Toutes les métadonnées sont stockées dans le répertoire $HOME/.sqoop.

##### Planification de tâches Sqoop dans Airflow
Le conteneur Hive-server héberge un serveur SSH.
[SSHOperator](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html) dans Airflow peut être utilisé pour se connecter au serveur Hive et exécuter une tâche Sqoop. Pour vous connecter à Hive-server depuis Airflow, définissez l'hôte sur « hive-server », le nom d'utilisateur sur « root » et le mot de passe sur celui du conteneur lors de la création d'une connexion SSH dans Airflow.

Voici la commande permettant de définir un mot de passe dans un conteneur :
```sh
passwd root
```

________________________________________________________________________________________________
## Exécution_d_Airflow
Pour planifier des tâches dans Airflow, des DAG doivent être créés.
Vos scripts DAG doivent être conservés dans le répertoire « /dags » du projet. Vos scripts se synchroniseront automatiquement avec le conteneur et vous pourrez consulter votre DAG via l'interface utilisateur d'Airflow.

Par défaut, SequentialExecutor est utilisé pour planifier les tâches. Cet exécuteur ne peut exécuter qu'une seule tâche à la fois. Si vous souhaitez planifier plusieurs tâches en parallèle, vous devrez configurer Airflow pour utiliser d'autres [exécuteurs](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html) tels que LocalExecutor.

SequentialExecutor utilise SQLite pour stocker les métadonnées. Si vous souhaitez utiliser un autre exécuteur tel que LocalExecutor, vous devrez peut-être configurer Airflow pour utiliser d'autres bases de données comme Postgres. Vous pouvez utiliser Postgres fourni avec ce cluster.

##### Informations d'identification Airflow
| nom | valeur |
|--- |--- |
| nom d'utilisateur | admin |
| mot de passe | admin |
| prénom | admin |
| nom | administrateur |
| rôle | Administrateur |
| e-mail | admin@gmail.com |

________________________________________________________________________________________________
## Créer une rubrique Kafka
Vous pouvez créer une rubrique Kafka depuis l'interface de ligne de commande (CLI) ou le centre de contrôle Kafka. Vous pouvez définir la valeur maximale du facteur de réplication sur 1, car ce cluster n'exécute qu'un seul broker Kafka.

##### CLI
Pour créer une rubrique Kafka via l'interface de ligne de commande (CLI), accédez au conteneur kafka-broker, puis exécutez la commande suivante :
```sh
kafka-topics --bootstrap-server kafka-broker:29092 --create --topic <topic-name> --partitions <int> --replication-factor <int>
```

##### ControlCenter_Kafka
Il s'agit d'une interface utilisateur fournie par Confluent. Pour y accéder, consultez la section [Accéder à l'écosystème Hadoop](#access-hadoop-ecosystem).

________________________________________________________________________________________________
## Lancement_producteur_Kafka
Vous pouvez créer un producteur Kafka depuis l'interface de ligne de commande (CLI) ou le centre de contrôle Kafka.

#### CLI
##### Utilisation du conteneur kafka-broker
Pour créer un producteur Kafka simple via l'interface de ligne de commande, accédez au conteneur « kafka-broker » et exécutez la commande suivante :
```sh
kafka-console-producer --broker-list kafka-broker:29092 --topic <topic-name> --producer-property <key>=<value>
```
> Remarque : La commande ci-dessus crée un producteur qui envoie des données sans clé.

##### Utilisation du conteneur schema-registry
Pour créer un producteur avro via l'interface de ligne de commande, accédez au conteneur « schema-registry » et exécutez la commande suivante. ```sh
kafka-avro-console-producer \
--broker-list kafka-broker:29092 \
--topic <topic-name> \
--property schema.registry.url=http://schema-registry:8083 \
--property value.schema='<avro-schema>'
```
> Remarque : Ce producteur ne crée pas de schéma dans schema-registry. Il utilise directement le schéma fourni dans la commande ci-dessus. La commande ci-dessus accepte le schéma de valeur.

### Kadmin
Vous pouvez créer un producteur simple ou un producteur avro avec Kadmin.

## Créer un consommateur Kafka
Vous pouvez créer un consommateur kakfka depuis l'interface de ligne de commande ou avec KAdmin.

### CLI
##### Utilisation du conteneur kafka-broker
Pour créer un consommateur Kafka simple via l'interface de ligne de commande, accédez au conteneur « kafka-broker » et exécutez la commande suivante :
```sh
kafka-console-consumer --bootstrap-server kafka-broker:29092 --topic <topic-name> --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true
```
> Remarque : La commande ci-dessus crée un consommateur qui lit depuis le début.

##### Utilisation du conteneur schema-registry
Pour créer un consommateur avro via l'interface de ligne de commande, accédez au conteneur « schema-registry » et exécutez la commande suivante.
```sh
kafka-avro-console-consumer \
--bootstrap-server kafka-broker:29092 \
--topic <topic-name> \
--from-beginning \
--property schema.registry.url=http://schema-registry:8083
```
> Remarque : La commande ci-dessus crée un consommateur qui lit depuis le début.

### Kadmin
Vous pouvez créer un consommateur simple ou un consommateur Avro avec Kadmin.

________________________________________________________________________________________________
## Exécution des agents Flume
[Flume](https://flume.apache.org/FlumeUserGuide.html) est installé dans « namenode ». Pour configurer les agents Flume, vous devez configurer le fichier « flume_config/flume.conf » dans le répertoire du projet. Pour démarrer les agents Flume, accédez à namenode et exécutez la commande ci-dessous.
```sh
flume-ng agent --conf conf --conf-file /opt/flume/conf/flume.conf --name <agent-name> -Dflume.root.logger=INFO,console
```
La commande ci-dessus exécute Flume dans la console.

Vous pouvez également créer un service Flume et le gérer en tant que tel.

________________________________________________________________________________________________
## Exécution de requêtes dans Cassandra
Accédez au conteneur Cassandra et exécutez la commande ci-dessous pour lancer l'exécution de requêtes CQL.
```sh
cqlsh
```

________________________________________________________________________________________________
## Jars disponibles
Voici une liste de fichiers jar supplémentaires disponibles et utilisables dans « spark ».
Ces fichiers jar se trouvent dans le répertoire `/opt/spark/jars/` du conteneur `namenode`.
| ID de groupe | ID d'artefact | Nom de fichier | Version |
| ---| ---| ---| ---|
| com.sun.jersey | jersey-client | jersey-client-1.19.4.jar | 1.19.4 |
| com.sun.jersey | jersey-bundle | jersey-bundle-1.19.4.jar | 1.19.4 |
| com.datastax.spark | spark-cassandra-connector-assembly_2.12 | spark-cassandra-connector-assembly_2.12-3.2.0.jar | 3.2.0 |
| mysql | mysql-connector-java | mysql-connector-java-5.1.49.jar | 5.1.49 |
| org.apache.spark | spark-sql-kafka-0-10_2.12 | spark-sql-kafka-0-10_2.12-3.2.1.jar | 3.2.1 |
| org.apache.kafka | kafka-clients | kafka-clients-3.2.2.jar | 3.2.2 |
| org.apache.spark | spark-avro_2.12 | spark-avro_2.12-3.2.2.jar | 3.2.2 |
| org.apache.spark | spark-token-provider-kafka-0-10_2.12 | spark-token-provider-kafka-0-10_2.12-3.2.2.jar | 3.2.2 |
| org.apache.commons | commons-pool2 | commons-pool2-2.11.1.jar | 2.11.1 |
| org.apache.avro | avro-mapred | avro-mapred-1.11.1.jar | 1.11.1 |
| org.postgresql | postgresql | postgresql-42.5.0.jar | 42.5.0 |

________________________________________________________________________________________________
## Configuration de Hadoop
Vous pouvez configurer l'allocation de ressources à chaque conteneur en modifiant le fichier « hadoop.env » dans le répertoire du projet.

##### Configuration de YARN
- YARN_CONF_yarn_scheduler_capacity_root_default_maximum___allocation___mb : Détermine l'allocation mémoire maximale au planificateur YARN. Par défaut, la valeur est de 8 Go. Vous pouvez l'augmenter si vous disposez de ressources supplémentaires.
- YARN_CONF_yarn_scheduler_capacity_root_default_maximum___allocation___vcores : Détermine le nombre maximal de cœurs alloués au planificateur YARN. Par défaut, la valeur est de 4 cœurs. Vous pouvez l'augmenter si vous disposez de ressources supplémentaires.

##### Configurer le gestionnaire de nœuds
- YARN_CONF_yarn_nodemanager_resource_memory___mb : Détermine l'allocation mémoire maximale au gestionnaire de nœuds. Par défaut, la valeur est de 16 Go. Il est conseillé de ne pas la diminuer, sinon les tâches de réduction de la carte pourraient se bloquer en raison d'une indisponibilité des ressources.
- YARN_CONF_yarn_nodemanager_resource_cpu___vcores : Détermine le nombre maximal de cœurs alloués au gestionnaire de nœuds. Par défaut, la valeur est de 8 cœurs. Il est conseillé de ne pas la diminuer.

##### Configurer la réduction de la carte
- MAPRED_CONF_mapreduce_map_memory_mb : Détermine l’allocation mémoire maximale pour un mappeur. Par défaut, elle est fixée à 4 Go. Vous pouvez l’augmenter si vous disposez de ressources supplémentaires.
- MAPRED_CONF_mapreduce_reduce_memory_mb : Détermine l’allocation mémoire maximale pour un réducteur. Par défaut, elle est fixée à 8 Go. Vous pouvez l’augmenter si vous disposez de ressources supplémentaires.

________________________________________________________________________________________________
## Configurer HUE
Vous pouvez configurer le serveur Hue en modifiant le fichier « hue-overrides.ini » dans le répertoire du projet.

##### Configurer la base de données Hue
Hue est configuré pour utiliser Postgres dans le conteneur « huedb ». La configuration par défaut est la suivante : ```sh
engine=postgresql_psycopg2
host=huedb
port=5432
user=hue
password=hue
name=hue
```

##### Configurer Hue pour exécuter Hive
Hue se connecte au conteneur `hive-server` via le port `10000`. Voici la configuration pour ce même conteneur.
```sh
[beeswax]

# Hôte sur lequel HiveServer2 est exécuté.
# Si la sécurité Kerberos est activée, utilisez un nom de domaine complet (FQDN).
hive_server_host=hive-server

# Port sur lequel le serveur Thrift HiveServer2 est exécuté.
hive_server_port=10000

thrift_version=7
```

________________________________________________________________________________________________
## Configurer Zeppelin
Vous pouvez [configurer](https://zeppelin.apache.org/docs/0.8.0/setup/operation/configuration.html) Zeppelin en modifiant les fichiers `configs/zeppelin-env.sh` et `configs/zeppelin-site.xml`.

##### Variables d'environnement
Vous trouverez ci-dessous les variables définies dans `zeppelin-env.sh`, utilisé par Zeppelin.
| Variable | Valeur |
|--- |--- |
| JAVA_HOME | /usr/lib/jvm/java-8-openjdk-amd64/ |
| SPARK_MASTER | spark://namenode:7077 |
| ZEPPELIN_PORT | 8082 |
| SPARK_HOME | /opt/spark |
| HADOOP_CONF_DIR | /etc/hadoop | | PYSPARK_PYTHON | /root/anaconda/bin/python3.9 |
| PYTHONPATH | /root/anaconda/bin/python3.9 |

##### Propriétés Java
Les propriétés Java peuvent être configurées depuis le fichier `zeppelin-site.xml`. J'ai modifié sa configuration par défaut pour exposer le serveur en externe. Voici la configuration modifiée :
```sh
<property>
<name>zeppelin.server.addr</name>
<value>0.0.0.0</value>
<description>Adresse de liaison du serveur</description>
</property>
```

##### Configurer l'interpréteur Spark
Suivez les étapes ci-dessous :
1. Cliquez sur « Profil » en haut à droite de l'écran.
2. Cliquez sur « Interprète ».
3. Faites défiler la page jusqu'à « spark » et cliquez sur « Modifier ».

________________________________________________________________________________________________
## Configurer Airflow
Vous pouvez configurer Airflow en modifiant le fichier « configs/namenode_airflow.cfg » dans le répertoire du projet.

##### Variables de configuration
Voici les variables que j'ai configurées. Vous pouvez laisser les autres variables telles quelles.
| Variable | Valeur |
|--- |--- |
| dags_folder | /root/airflow/dags |
| sql_alchemy_conn | sqlite:////root/airflow/airflow.db |
| [executor](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html) | SequentialExecutor |

Vous pouvez utiliser d'autres exécuteurs, tels que « LocalExecutor ». Pour utiliser cet exécuteur, vous devez créer une instance de base de données sur un serveur de base de données. Vous pouvez utiliser le conteneur « external_postgres_db ». Une fois la base de données créée, définissez sql_alchemy_conn sur postgresql+psycopg2://<username>:<password>@[IP/container-name]:5432/[db-name] et exécuteur sur LocalExecutor.

##### Recharger Airflow
Après avoir configuré les variables, vous devez initialiser une nouvelle instance de base de données et redémarrer le serveur Airflow et le planificateur.

Connectez-vous à `namenode` en exécutant cette [commande](#access-docker-containers) et exécutez la commande ci-dessous pour initialiser une base de données.
```sh
airflow db init && \
airflow users create \
--username admin \
--password admin \
--firstname admin \
--lastname admin \
--role Admin \
--email admin@gmail.com
```

Exécutez la commande ci-dessous pour redémarrer le serveur Airflow et le planificateur.
```sh
rm -rf ~/airflow/airflow-scheduler.pid && \
rm -rf ~/airflow/airflow-webserver-monitor.pid && \
rm -rf ~/airflow/airflow-webserver.pid && \
serveur web airflow -p 3000 -D --workers 1 && \
planificateur airflow -D
```
> Inutile d'exécuter cette commande au prochain démarrage de votre cluster.

________________________________________________________________________________________________
## Configuration de sqoop
Vous pouvez configurer sqoop en modifiant le fichier `configs/hive_server/sqoop-site.xml` dans le répertoire du projet.

##### Propriétés Java
J'ai modifié sa configuration par défaut afin qu'elle puisse enregistrer le mot de passe du serveur de base de données dans le metastore. Voici la configuration modifiée.
```sh
<property>
<name>sqoop.metastore.client.record.password</name>
<value>true</value>
<description>Si true, autorise les mots de passe enregistrés dans le metastore.
</description>
</property>
```

________________________________________________________________________________________________
## Configurer le serveur SSH
Un serveur SSH s'exécute sur le conteneur Hive-Server. Vous pouvez le configurer en modifiant le fichier `configs/hive_server/sshd_config.conf` dans le répertoire du projet. Je l'ai configuré pour autoriser l'authentification par mot de passe et la connexion en tant que root.

________________________________________________________________________________________________
## Créer des images Docker personnalisées
Vous pouvez personnaliser les images utilisées dans ces fichiers YAML Docker. Le répertoire `docker_image_conf` contient les fichiers Docker pour la création d'images.

________________________________________________________________________________________________
## Gérer les ressources
Je recommande d'utiliser un poste de travail doté d'au moins 4 cœurs hyperthreadés et de 8 Go de RAM pour exécuter ce cluster. De plus, je déconseille d'exécuter d'autres programmes simultanément, car cela pourrait entraîner des pénuries de ressources et d'autres problèmes lors de l'utilisation de certains ports.

##### Réduire l'utilisation des ressources
1. Si vous n'utilisez pas Hive, je vous conseille de démarrer le cluster de base.
2. Vous pouvez démarrer le cluster sans le conteneur external_pgadmin si vous n'avez pas besoin d'exécuter de requêtes SQL sur Postgres.
3. Si vous n'avez pas besoin de Cassandra, lancez le cluster sans.

##### Spark et Zeppelin
1. Lorsque vous exécutez votre code PySpark dans Zeppelin, un pilote Spark démarre, ce qui consomme beaucoup de ressources. Je recommande d'exécuter `spark.stop()` et `sc.stop()` dans le bloc suivant de Zeppelin. Cela arrêtera le pilote Spark.
2. Si le pilote consomme encore des ressources, vous pouvez vous connecter à `namenode` et exécuter `zeppelin-daemon.sh stop`. Cela arrêtera Zeppelin.

________________________________________________________________________________________________
## Dépendances
- [Docker](https://docs.docker.com/) :
1. Installer Docker sur Ubuntu : https://docs.docker.com/engine/install/ubuntu/
2. Installer Docker sur Windows : https://docs.docker.com/desktop/install/windows-install/

________________________________________________________________________________________________
[Site web] : https://mrayonline.web.app
[YouTube] : https://www.youtube.com/@mrugankray7623
[Linkedin] : https://in.linkedin.com/in/mrugank-ray-543886149
[buy_me_coffee] : https://www.buymeacoffee.com/mrugankray
