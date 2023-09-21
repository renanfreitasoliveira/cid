from pyspark_llap import HiveWarehouseSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, MapType, ArrayType
from pyspark.sql.functions import substring, format_string, col, lpad, to_date, date_format, from_json, concat_ws, lit, when, sum, broadcast, trim, coalesce, to_timestamp, rpad, unix_timestamp, from_unixtime, split, rank, upper, translate, regexp_replace, expr, current_date, length, lower, current_timestamp, countDistinct, year, month
from pyspark.sql.window import Window
import re
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import split, concat_ws
from pyspark.sql.functions import concat, explode
from pyspark.sql.functions import row_number

spark = SparkSession.builder.config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT').config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT').config('spark.sql.session.timeZone', 'GMT').enableHiveSupport().getOrCreate()
hwc = HiveWarehouseSession.session(spark).build()

try:
        tabela_capitulos = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/capitulos.csv")

        tabela_grupos = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/grupos.csv")

        tabela_categorias = spark.read.option("header", "true").option("delimiter", ",").csv(r'file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/categorias.csv').withColumnRenamed("CID-10","CID1").withColumnRenamed("CATEGORIA","Categoria").drop("_c2")

        tabela_subcategorias = spark.read.option("header", "true").option("delimiter", ",").csv(r'file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/subcategorias.csv').withColumnRenamed("SUBCATEGORIA","subcategorias").withColumnRenamed("CID-10","cid_sub")

        tabela_cap_gru_cat = tabela_capitulos.join(tabela_grupos, tabela_capitulos['CID'] == tabela_grupos['CID_10'], how='left')

        tabela_cap_gru_cat = tabela_cap_gru_cat.drop('CID_10')

        tabela_cap_gru_cat = tabela_cap_gru_cat.join(tabela_categorias, F.when(F.length(tabela_cap_gru_cat.CID) == 4, F.substring(tabela_cap_gru_cat.CID, 1, 3)).otherwise(tabela_cap_gru_cat.CID) == tabela_categorias.CID1, "left").drop("CID1")

        tabela_cap_gru_cat  = tabela_cap_gru_cat.withColumn("Categoria", F.when(tabela_cap_gru_cat["Categoria"].isNull(), "Categoria não definida").otherwise(tabela_cap_gru_cat["Categoria"]))

        tabela_subcategorias_proxy = tabela_subcategorias.withColumnRenamed('subcategorias', 'sub')

        tabela_subcategorias = tabela_subcategorias.withColumn("CID_novo", substring(col("cid_sub"), 1, 3)).drop("cid_sub")

        tabela_semi = tabela_cap_gru_cat.join(tabela_subcategorias, tabela_cap_gru_cat['CID'] == tabela_subcategorias['CID_novo'], how = 'left')

        tabela_final = tabela_semi.join(tabela_subcategorias_proxy, tabela_semi['CID'] == tabela_subcategorias_proxy['cid_sub'], how='left')

        tabela_final = tabela_final.drop("CID_novo","cid_sub")

        tabela_final = tabela_final.withColumn("subcategoria_unificada", coalesce(tabela_final["subcategorias"], tabela_final["sub"])).drop('subcategorias','sub').withColumnRenamed('subcategoria_unificada', 'subcategoria')

        tabela_final = tabela_final.dropDuplicates()

        tabela_final = tabela_final.na.fill({"subcategoria": "Subcategoria não definida"})

        window_spec = Window.orderBy("CID")

        tabela_final = tabela_final.withColumn("row_number", row_number().over(window_spec))

        tabela_final  = tabela_final.withColumn("CID", format_string("A%03d", col("row_number")))

        tabela_final  = tabela_final.drop("row_number")

        tabela_final.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").mode("overwrite").option("table", "modelo_saude.cid").option("fileformat","parquet").save()
        
finally:
    try:
        spark.catalog.clearCache()
        hwc.close()
        spark.stop()
        print("[INFO_SIGMA] Job Spark finalizado")
    except Exception as e:
        print("[ERRO_SIGMA] Ocorreu um erro ao fechar as sessões: " + str(e)) 
        raise e    