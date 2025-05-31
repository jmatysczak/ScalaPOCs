export SPARK_WORKER_INSTANCES=2
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077 --cores 2 --memory 2G --work-dir /tmp/spark_work_dir
