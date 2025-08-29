#!/usr/bin/env bash
set -euo pipefail

# Aggiorna il sistema e installa le dipendenze di base
sudo apt update && sudo apt -y upgrade
sudo apt -y install openjdk-11-jdk python3 python3-venv python3-pip nfs-common git curl

# Configura il mount NFS
MANAGER_IP="10.0.1.7" # Assicurati che questo sia l'IP privato del tuo manager
sudo mkdir -p /data
# Monta la cartella condivisa dal manager. Aggiungi il flag "-o vers=4.1" se hai problemi di versione NFS
sudo mount -t nfs ${MANAGER_IP}:/data /data

# Rendi il mount persistente dopo il riavvio
echo "${MANAGER_IP}:/data /data nfs defaults 0 0" | sudo tee -a /etc/fstab

# Scarica e configura Spark Standalone
SPARK_VER="3.3.2"
HADOOP_PROFILE="hadoop3"
SPARK_TGZ="spark-${SPARK_VER}-bin-${HADOOP_PROFILE}.tgz"

sudo wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VER}/${SPARK_TGZ}" -O /tmp/${SPARK_TGZ}
sudo tar -C /opt -xzf /tmp/${SPARK_TGZ}

# Crea un link simbolico e imposta i permessi
sudo ln -s /opt/spark-${SPARK_VER}-bin-${HADOOP_PROFILE} /opt/spark
sudo useradd -m -s /bin/bash spark || true
sudo chown -R spark:spark /opt/spark-${SPARK_VER}-bin-${HADOOP_PROFILE} /opt/spark

# Imposta le variabili d'ambiente per tutti gli utenti
echo "export SPARK_HOME=/opt/spark" | sudo tee /etc/profile.d/spark.sh
echo 'export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH' | sudo tee -a /etc/profile.d/spark.sh

# Carica le variabili d'ambiente nella sessione corrente
source /etc/profile.d/spark.sh

echo "Worker provisioning completato"