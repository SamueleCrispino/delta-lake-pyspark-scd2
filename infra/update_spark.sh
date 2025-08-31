#!/bin/bash

# Define Spark versions and directory names
OLD_SPARK_VERSION="3.3.2"
NEW_SPARK_VERSION="3.5.0"
OLD_SPARK_DIR_NAME="spark-${OLD_SPARK_VERSION}-bin-hadoop3"
NEW_SPARK_DIR_NAME="spark-${NEW_SPARK_VERSION}-bin-hadoop3"
SPARK_TAR_NAME="${NEW_SPARK_DIR_NAME}.tgz"
SPARK_DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${NEW_SPARK_VERSION}/${SPARK_TAR_NAME}"
SPARK_HOME_PATH="/opt/spark"
OLD_SPARK_PATH="/opt/${OLD_SPARK_DIR_NAME}"
NEW_SPARK_PATH="/opt/${NEW_SPARK_DIR_NAME}"

# Set the list of worker nodes from the workers.txt file
WORKER_NODES=$(cat workers.txt)
MASTER_NODE="VM-1" # Or your master's IP

# --- Function to update Spark on a remote machine ---
update_remote_spark() {
    local remote_host=$1
    echo "--- Updating Spark on ${remote_host} ---"

    # Stop any running Spark services using the old path
    ssh -o StrictHostKeyChecking=no Crispinoadmin@${remote_host} "if [ -d ${OLD_SPARK_PATH} ]; then sudo ${OLD_SPARK_PATH}/sbin/stop-worker.sh; fi"

    # Clean up old Spark directory and symlink
    ssh -o StrictHostKeyChecking=no Crispinoadmin@${remote_host} "sudo rm -rf ${OLD_SPARK_PATH} && sudo rm -f ${SPARK_HOME_PATH}"

    # Download and install the new Spark version
    ssh -o StrictHostKeyChecking=no Crispinoadmin@${remote_host} "cd /opt/ && sudo wget ${SPARK_DOWNLOAD_URL} && sudo tar -xvf ${SPARK_TAR_NAME} && sudo rm ${SPARK_TAR_NAME}"

    # Create new symlink
    ssh -o StrictHostKeyChecking=no Crispinoadmin@${remote_host} "sudo ln -s ${NEW_SPARK_PATH} ${SPARK_HOME_PATH}"

    echo "--- Spark update complete on ${remote_host} ---"
}

# --- Main script execution ---

echo "Starting automated Spark cluster update..."

# 1. Stop all services on the master node using the old path
echo "Stopping all Spark services on the master node..."
if [ -d "${OLD_SPARK_PATH}" ]; then
    ${OLD_SPARK_PATH}/sbin/stop-all.sh
fi

# 2. Update Spark on the master node
update_remote_spark ${MASTER_NODE}

# 3. Update Spark on all worker nodes
for worker in ${WORKER_NODES}; do
    update_remote_spark ${worker}
done

# 4. Push updated configuration files (you may want to re-check these)
echo "Pushing configuration files to all worker nodes..."
for worker in ${WORKER_NODES}; do
    scp ${SPARK_HOME_PATH}/conf/spark-env.sh Crispinoadmin@${worker}:/tmp/
    ssh Crispinoadmin@${worker} "sudo mv /tmp/spark-env.sh ${SPARK_HOME_PATH}/conf/"
done

# 5. Start the cluster
echo "Starting the Spark cluster..."
${SPARK_HOME_PATH}/sbin/start-all.sh

echo "Spark cluster update and restart complete."