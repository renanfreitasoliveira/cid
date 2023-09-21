#!/bin/bash

## Dependencias externas comuns a todos os scripts
source "${COMMONS_PATH_SPARK}/index.sh"
source "${COMMONS_PATH_SPARK}/conf_spark_p.sh"

## Configuracoes de variaveis
CURRENT_DIR="$(get_current_dir)"
SPARK_SCRIPT_OPTIONS="cid"

inicializa_importacao $SPARK_SCRIPT_OPTIONS

spark-submit --py-files file:///opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip,file:///opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/pyspark.zip,file:///opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip --jars /opt/java_share/ImpalaJDBC42.jar,/opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar --conf hive.warehouse.metastoreUri=thrift://cdp02.tce.sc.gov.br:9083,thrift://cdp03.tce.sc.gov.br:9083 --conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://cdp01.tce.sc.gov.br:2181,cdp02.tce.sc.gov.br:2181,cdp03.tce.sc.gov.br:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --conf spark.security.credentials.hiveserver2.enabled=true --conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions --conf spark.sql.hive.hwc.execution.mode=spark --conf spark.sql.hive.hiveserver2.jdbc.url.principal=hive/_HOST@SCTCE.TRIBUNAL --conf spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator --conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster --conf spark.datasource.hive.warehouse.read.mode=DIRECT_READER_V2 --conf spark.datasource.hive.warehouse.read.via.llap=false --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.execution.arrow.pyspark.enabled=true --conf spark.port.maxRetries=100 --driver-class-path /opt/java_share/ImpalaJDBC42.jar --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=$MIN_EXECUTORS --conf spark.dynamicAllocation.maxExecutors=$MAX_EXECUTORS --conf spark.dynamicAllocation.initialExecutors=$INITIAL_EXECUTORS --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.dynamicAllocation.shuffleTracking.enabled=true --executor-cores $EXECUTORS_CORES --driver-cores $DRIVER_CORES --driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY   "${CURRENT_DIR}/$SPARK_SCRIPT_OPTIONS.py"

if [ $? -gt 0 ]; then
    echo "[INFO]: Processo de importacao finalizado com ERRO [$SPARK_SCRIPT_OPTIONS]"
    echo "[ERROR]: 1"
    finaliza_importacao $SPARK_SCRIPT_OPTIONS
    exit 1
fi

finaliza_importacao $SPARK_SCRIPT_OPTIONS
