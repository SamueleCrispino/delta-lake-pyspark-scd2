#!/usr/bin/env bash
set -euo pipefail

# Aggiorna
sudo apt update && sudo apt -y upgrade

# 1) utils di base
sudo apt -y install curl wget unzip git build-essential software-properties-common apt-transport-https ca-certificates

# 2) Java 11
sudo apt -y install openjdk-11-jdk
# verifica: java -version

# 3) Python 3 + pip + virtualenv
sudo apt -y install python3 python3-venv python3-pip
python3 -m pip install --upgrade pip

# 4) NFS server (condivide /data)
sudo apt -y install nfs-kernel-server
sudo mkdir -p /data
sudo chown nobody:nogroup /data
sudo chmod 0775 /data
# exporta /data a tutti (metti rete privata se preferisci limitare)
echo "/data *(rw,sync,no_subtree_check,no_root_squash)" | sudo tee /etc/exports
sudo exportfs -ra
sudo systemctl enable --now nfs-kernel-server

# 5) Spark standalone (scegli versione compatibile)
# consiglio Spark 3.3.2 + Scala 2.12 (compatibile con delta-spark_2.12:3.1.0)
# 5) Spark standalone (scegli versione compatibile)
SPARK_VER="3.3.2"
HADOOP_PROFILE="bin-hadoop3"
SCALA_VER="2.12"
SPARK_TGZ="spark-${SPARK_VER}-${HADOOP_PROFILE}.tgz"

sudo wget "https://archive.apache.org/dist/spark/spark-${SPARK_VER}/${SPARK_TGZ}" -O /tmp/${SPARK_TGZ}
sudo tar -C /opt -xzf /tmp/${SPARK_TGZ}
sudo ln -s /opt/spark-${SPARK_VER}-${HADOOP_PROFILE} /opt/spark
sudo useradd -m -s /bin/bash spark || true
sudo chown -R spark:spark /opt/spark-${SPARK_VER}-${HADOOP_PROFILE} /opt/spark

# Variabili d'ambiente per tutti gli utenti
echo "export SPARK_HOME=/opt/spark" | sudo tee /etc/profile.d/spark.sh
echo 'export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH' | sudo tee -a /etc/profile.d/spark.sh


# 6) Crea cartella progetto sul shared storage
sudo mkdir -p /data/project
sudo chown -R $USER:$USER /data/project

# 7) Abilita firewall per SSH solo (opzionale)
# sudo apt -y install ufw
# sudo ufw allow OpenSSH
# sudo ufw --force enable

echo "Provisioning manager completato. Controlla /data e SPARK_HOME"
