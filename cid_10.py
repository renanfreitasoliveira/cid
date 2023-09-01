from pyspark_llap import HiveWarehouseSession
from pyspark.sql.types import StructType,StructField, StringType,IntegerType,DateType, MapType,ArrayType, DoubleType, LongType
from pyspark.sql.functions import substring, format_string, col, lpad, to_date, date_format, from_json,concat_ws,lit, when, sum, broadcast,trim,coalesce,to_timestamp,rpad,unix_timestamp, from_unixtime,current_date, regexp_replace, current_timestamp, year, lit, concat, split, length, substring, expr, first
from pyspark.sql import functions as F
import re
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT').config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT').config('spark.sql.session.timeZone', 'GMT').enableHiveSupport().getOrCreate()
hwc = HiveWarehouseSession.session(spark).build()

# 1 - renomeia colunas do SIA origem_saude
try:

        #Importando os arquivos do CID-10

        capitulos = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/CID-10-CAPITULOS.CSV")

        grupos = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/CID-10-GRUPOS.CSV")

        categorias = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/CID-10-CATEGORIAS.CSV")

        subcategorias = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/CID-10-SUBCATEGORIAS.CSV")

        # Fazendo a limpeza do dataframe 'capitulos'
        capitulos = capitulos.drop('Unnamed: 5', 'DESCRABREV', 'NUMCAP', 'CATFIM')
        capitulos = capitulos.withColumnRenamed('CATINIC', 'CID-10').withColumnRenamed('DESCRICAO', 'CAPITULOS')

        # Fazendo a limpeza do dataframe 'categorias'
        categorias = categorias.drop('Unnamed: 6', 'EXCLUIDOS', 'REFER', 'CLASSIF', 'DESCRABREV')
        categorias = categorias.withColumnRenamed('DESCRICAO', 'CATEGORIAS').withColumnRenamed('CAT', 'CID-10')

        # Fazendo a limpeza do dataframe 'grupos'
        grupos = grupos.drop('Unnamed: 4', 'DESCRABREV', 'CATFIM')
        grupos = grupos.withColumnRenamed('CATINIC', 'CID-10').withColumnRenamed('DESCRICA0', 'GRUPOS')

        # Fazendo a limpeza do dataframe 'subcategorias'
        subcategorias = subcategorias.drop('Unnamed: 8', 'DESCRABREV', 'REFER', 'CAUSAOBITO', 'CLASSIF', 'RESTRSEXO', 'EXCLUIDOS')
        subcategorias = subcategorias.withColumnRenamed('DESCRICAO', 'SUBCATEGORIAS').withColumnRenamed('SUBCAT', 'CID-10')

        # Combine as DataFrames usando o método union
        tabela_concatenada = capitulos.union(categorias).union(grupos).union(subcategorias)

        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Tabela que relaciona o intervalo inicial, intervalo final e descrição de cada capitulo
        tabela_capitulos = [
            (("A00", "B99"), "Capítulo I - Algumas doenças infecciosas e parasitárias"),
            (("C00", "D48"), "Capítulo II - Neoplasias [tumores]"),
            (("D50", "D89"), "Capítulo III - Doenças do sangue e dos órgãos hematopoéticos e alguns transtornos imunitários"),
            (("E00", "E90"), "Capítulo IV - Doenças endócrinas, nutricionais e metabólicas"),
            (("F00", "F99"), "Capítulo V - Transtornos mentais e comportamentais"),
            (("G00", "G99"), "Capítulo VI - Doenças do sistema nervoso"),
            (("H00", "H59"), "Capítulo VII - Doenças do olho e anexos"),
            (("H60", "H95"), "Capítulo VIII - Doenças do ouvido e da apófise mastóide"),
            (("I00", "I99"), "Capítulo IX - Doenças do aparelho circulatório"),
            (("J00", "J99"), "Capítulo X - Doenças do aparelho respiratório"),
            (("K00", "K93"), "Capítulo XI - Doenças do aparelho digestivo"),
            (("L00", "L99"), "Capítulo XII - Doenças da pele e do tecido subcutâneo"),
            (("M00", "M99"), "Capítulo XIII - Doenças do sistema osteomuscular e do tecido conjuntivo"),
            (("N00", "N99"), "Capítulo XIV - Doenças do aparelho geniturinário"),
            (("O00", "O99"), "Capítulo XV - Gravidez, parto e puerpério"),
            (("P00", "P96"), "Capítulo XVI - Algumas afecções originadas no período perinatal"),
            (("Q00", "Q99"), "Capítulo XVII - Malformações congênitas, deformidades e anomalias cromossômicas"),
            (("R00", "R99"), "Capítulo XVIII - Sintomas, sinais e achados anormais de exames clínicos e de laboratório, não classificados em outra parte"),
            (("S00", "T98"), "Capítulo XIX - Lesões, envenenamento e algumas outras consequências de causas externas"),
            (("V01", "Y98"), "Capítulo XX - Causas externas de morbidade e de mortalidade"),
            (("Z00", "Z99"), "Capítulo XXI - Fatores que influenciam o estado de saúde e o contato com os serviços de saúde"),
            (("U04", "U99"), "Capítulo XXII - Códigos para propósitos especiais")
        ]


        # Filtrar os valores com 4 caracteres
        filtered_df = tabela_concatenada.filter(length(col("CID-10")) == 4)

        # Função para classificar os códigos nos capítulos
        def classificar_capitulo(codigo):
            for intervalo, capitulo in tabela_capitulos:
                if intervalo[0] <= codigo <= intervalo[1]:
                    return capitulo
            return "Capítulo não encontrado"

        # Registre a função como uma UDF (User-Defined Function)
        classificar_capitulo_udf = udf(classificar_capitulo, StringType())

        # Adicione uma coluna com os capítulos correspondentes
        df_with_capitulos = filtered_df.withColumn("Capítulo", classificar_capitulo_udf(col("CID-10")))

        # Tabela que relaciona o intervalo inicial, intervalo final e descrição de cada grupo
        tabela_grupos = [
            (("A00", "A09"), "Doenças infecciosas intestinais"),
            (("A15", "A19"), "Tuberculose"),
            (("A20", "A28"), "Algumas doenças bacterianas zoonóticas"),
            (("A30", "A49"), "Outras doenças bacterianas"),
            (("A50", "A64"), "Infecções de transmissão predominantemente sexual"),
            (("A65", "A69"), "Outras doenças por espiroquetas"),
            (("A70", "A74"), "Outras doenças causadas por clamídias"),
            (("A75", "A79"), "Rickettsioses"),
            (("A80", "A89"), "Infecções virais do sistema nervoso central"),
            (("A90", "A99"), "Febres por arbovírus e febres hemorrágicas virais"),
            (("B00", "B09"), "Infecções virais caracterizadas por lesões de pele e mucosas"),
            (("B15", "B19"), "Hepatite viral"),
            (("B20", "B24"), "Doença pelo vírus da imunodeficiência humana [HIV]"),
            (("B25", "B34"), "Outras doenças por vírus"),
            (("B35", "B49"), "Micoses"),
            (("B50", "B64"), "Doenças devidas a protozoários"),
            (("B65", "B83"), "Helmintíases"),
            (("B85", "B89"), "Pediculose, acaríase e outras infestações"),
            (("B90", "B94"), "Seqüelas de doenças infecciosas e parasitárias"),
            (("B95", "B97"), "Agentes de infecções bacterianas, virais e outros agentes infecciosos"),
            (("B99", "B99"), "Outras doenças infecciosas"),
            (("C00", "C97"), "Neoplasias [tumores] malignas(os)"),
            (("C00", "C75"), "Neoplasias [tumores] malignas(os), declaradas ou presumidas como primárias, de localizações especificadas, exceto dos tecidos linfático, hematopoético e tecidos correlatos"),
            (("C00", "C14"), "Neoplasias malignas do lábio, cavidade oral e faringe"),
            (("C15", "C26"), "Neoplasias malignas dos órgãos digestivos"),
            (("C30", "C39"), "Neoplasias malignas do aparelho respiratório e dos órgãos intratorácicos"),
            (("C40", "C41"), "Neoplasias malignas dos ossos e das cartilagens articulares"),
            (("C43", "C44"), "Melanoma e outras(os) neoplasias malignas da pele"),
            (("C45", "C49"), "Neoplasias malignas do tecido mesotelial e tecidos moles"),
            (("C50", "C50"), "Neoplasias malignas da mama"),
            (("C51", "C58"), "Neoplasias malignas dos órgãos genitais femininos"),
            (("C60", "C63"), "Neoplasias malignas dos órgãos genitais masculinos"),
            (("C64", "C68"), "Neoplasias malignas do trato urinário"),
            (("C69", "C72"), "Neoplasias malignas dos olhos, do encéfalo e de outras partes do sistema nervoso central"),
            (("C73", "C75"), "Neoplasias malignas da tireóide e de outras glândulas endócrinas"),
            (("C76", "C80"), "Neoplasias malignas de localizações mal definidas, secundárias e de localizações não especificadas"),
            (("C81", "C96"), "Neoplasias [tumores] malignas(os), declaradas ou presumidas como primárias, dos tecidos linfático, hematopoético e tecidos correlatos"),
            (("C97", "C97"), "Neoplasias malignas de localizações múltiplas independentes (primárias)"),
            (("D00", "D09"), "Neoplasias [tumores] in situ"),
            (("D10", "D36"), "Neoplasias [tumores] benignas(os)"),
            (("D37", "D48"), "Neoplasias [tumores] de comportamento incerto ou desconhecido"),
            (("D50", "D53"), "Anemias nutricionais"),
            (("D55", "D59"), "Anemias hemolíticas"),
            (("D60", "D64"), "Anemias aplásticas e outras anemias"),
            (("D65", "D69"), "Defeitos da coagulação, púrpura e outras afecções hemorrágicas"),
            (("D70", "D77"), "Outras doenças do sangue e dos órgãos hematopoéticos"),
            (("D80", "D89"), "Alguns transtornos que comprometem o mecanismo imunitário"),
            (("E00", "E07"), "Transtornos da glândula tireóide"),
            (("E10", "E14"), "Diabetes mellitus"),
            (("E15", "E16"), "Outros transtornos da regulação da glicose e da secreção pancreática interna"),
            (("E20", "E35"), "Transtornos de outras glândulas endócrinas"),
            (("E40", "E46"), "Desnutrição"),
            (("E50", "E64"), "Outras deficiências nutricionais"),
            (("E65", "E68"), "Obesidade e outras formas de hiperalimentação"),
            (("E70", "E90"), "Distúrbios metabólicos"),
            (("F00", "F09"), "Transtornos mentais orgânicos, inclusive os sintomáticos"),
            (("F10", "F19"), "Transtornos mentais e comportamentais devidos ao uso de substância psicoativa"),
            (("F20", "F29"), "Esquizofrenia, transtornos esquizotípicos e transtornos delirantes"),
            (("F30", "F39"), "Transtornos do humor [afetivos]"),
            (("F40", "F48"), "Transtornos neuróticos, transtornos relacionados com o 'stress' e transtornos somatoformes"),
            (("F50", "F59"), "Síndromes comportamentais associadas a disfunções fisiológicas e a fatores físicos"),
            (("F60", "F69"), "Transtornos da personalidade e do comportamento do adulto"),
            (("F70", "F79"), "Retardo mental"),
            (("F80", "F89"), "Transtornos do desenvolvimento psicológico"),
            (("F90", "F98"), "Transtornos emocionais e do comportamento que aparecem habitualmente na infância e na adolescência"),
            (("F99", "F99"), "Transtornos mentais não especificados"),
            (("G00", "G99"), "Doenças do sistema nervoso"),
            (("H00", "H59"), "Doenças do olho e anexos"),
            (("I00", "I99"), "Doenças do aparelho circulatório"),
            (("J00", "J99"), "Doenças do aparelho respiratório"),
            (("K00", "K93"), "Doenças do aparelho digestivo"),
            (("L00", "L99"), "Doenças da pele e do tecido subcutâneo"),
            (("M00", "M99"), "Doenças do sistema osteomuscular e do tecido conjuntivo"),
            (("N00", "N99"), "Doenças do sistema geniturinário"),
            (("O00", "O99"), "Gravidez, parto e puerpério"),
            (("P00", "P96"), "Algumas afecções originadas no período perinatal"),
            (("Q00", "Q99"), "Malformações congênitas, deformidades e anomalias cromossômicas"),
            (("R00", "R99"), "Sintomas, sinais e achados anormais de exames clínicos e de laboratório não classificados em outra parte"),
            (("S00", "T98"), "Lesões, envenenamentos e algumas outras consequências de causas externas"),
            (("U04", "U99"), "CID 10ª Revisão não disponível (designação provisória de novas doenças, agentes bacterianos resistentes a antibióticos e outros)"),
            (("V01", "Y98"), "Causas externas de morbidade e de mortalidade"),
            (("Z00", "Z99"), "Fatores que influenciam o estado de saúde e o contato com os serviços de saúde em circunstâncias específicas")
        ]


        # Filtrar os valores com 4 caracteres
        filtered_df1 = df_with_capitulos.filter(length(col("CID-10")) == 4)


        # Função para classificar os códigos nos grupos
        def classificar_grupo(codigo):
            for intervalo, grupo in tabela_grupos:
                if intervalo[0] <= codigo <= intervalo[1]:
                    return grupo
            return "Grupo não encontrado"

        # Registre a função como uma UDF (User-Defined Function)
        classificar_grupo_udf = udf(classificar_grupo, StringType())

        # Adicione uma coluna com os grupos correspondentes
        df_with_grupos = df_with_capitulos.withColumn("Grupo", classificar_grupo_udf(col("CID-10")))

        tabela_categorias = spark.read.option("header", "true").option("delimiter", ",").csv(r'file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/categorias.csv')

        tabela_subcategorias = spark.read.option("header", "true").option("delimiter", ",").csv(r'file:///mnt/carga_share/origem_datasus/cid/arquivos_cid10/subcategorias.csv')

        # Realizar a classificação usando expressões Spark SQL
        df_with_grupos_classificado = df_with_grupos.join(tabela_categorias, 
                                                        df_with_grupos["CID-10"].substr(1, 3) == tabela_categorias["CID-10"], 
                                                        "left_outer") \
                                                    .select(df_with_grupos["*"], tabela_categorias["CATEGORIA"])

        df_with_grupos_classificado = df_with_grupos_classificado.drop("_c5")
        df_with_grupos_classificado = df_with_grupos_classificado.drop("CAPITULOS")

        def classificar_subcategorias(cid_value, subcategorias_map):
            # Se o valor CID-10 tiver 4 dígitos, use-os diretamente para classificação
            if len(cid_value) == 4 and cid_value in subcategorias_map:
                return subcategorias_map[cid_value]
            
            # Se o valor CID-10 tiver 3 dígitos, tente usar os 3 primeiros dígitos para classificação
            elif len(cid_value) == 3:
                possible_cid_3_digitos = cid_value
                if possible_cid_3_digitos in subcategorias_map:
                    return subcategorias_map[possible_cid_3_digitos]
            
            # Se nenhuma correspondência for encontrada, retorne None ou algum valor de fallback
            return None

        # Suponha que você já tenha carregado as tabelas e os dados

        # Crie um dicionário mapeando valores CID-10 para subcategorias da tabela_subcategorias
        subcategorias_map = tabela_subcategorias.rdd.collectAsMap()

        # Registre a função UDF (User Defined Function)
        classificar_udf = udf(lambda cid: classificar_subcategorias(cid, subcategorias_map), StringType())

        # Adicione uma coluna classificada à tabela df_with_grupos_classificado
        df_with_classificacao = df_with_grupos_classificado.withColumn("SUBCATEGORIAS", classificar_udf(col("CID-10")))

        #Limpando a tabela completa

        df_with_classificacao = df_with_classificacao.withColumnRenamed("CID-10", "CID_10").withColumnRenamed("Grupo", "GRUPOS").withColumnRenamed("Capítulo", "CAPITULOS").withColumnRenamed("CATEGORIA", "CATEGORIAS")

        # Supondo que as colunas são 'Capitulos', 'Grupos', 'Categorias' e 'Subcategorias'
        nova_ordem_colunas = ['CID_10','CAPITULOS', 'GRUPOS', 'CATEGORIAS', 'SUBCATEGORIAS']

        # Use a função select para reorganizar as colunas
        df_with_classificacao= df_with_classificacao.select(*nova_ordem_colunas)

        #Exportando arquivo para o banco de dados

        df_with_classificacao.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").mode("overwrite").option("table", "modelo_saude.cid").option("fileformat","parquet").save()

finally:
    try:
        spark.catalog.clearCache()
        hwc.close()
        spark.stop()
        print("[INFO_SIGMA] Job Spark finalizado")
    except Exception as e:
        print("[ERRO_SIGMA] Ocorreu um erro ao fechar as sessões: " + str(e)) 
        raise e    