import xlrd 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, explode, array, struct, expr, sum, lit  
from pyspark.sql.functions import concat, concat_ws, lit, col, trim, to_date
import datetime
from pathlib import Path 

def sparkLoadExcelToParquet(inputFileName='vendas-combustiveis-m3.xls', outputfilename = None):
    filepath = Path('/opt/airflow/dags/'+ inputFileName)
    wb = xlrd.open_workbook(filepath)

    sheet_names = [name for name in wb.sheet_names() if name != 'Plan1']
    data = []
    columns = wb.sheet_by_name(sheet_names[0]).row_values(0)
    for sheet_name in sheet_names:
        sheet = wb.sheet_by_name(sheet_name)
        nrows = sheet.nrows
        for i in range(nrows):
            if i > 0:
                data.append(sheet.row_values(i))

    for i in range(len(data)):
        for k in range(len(data[i])):
            if data[i][k] == '':
                data[i][k] = 0.0

    spark = SparkSession.builder.master("local[*]").appName('sparkdf').getOrCreate()
    df = spark.createDataFrame(data, columns).drop('TOTAL')
    df = df.withColumn("ANO", F.round(df["ANO"]).cast('integer'))            

    def to_explode(df, by):
        # Filter dtypes and split into column names and type description
        cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
        # Spark SQL supports only homogeneous columns
        assert len(set(dtypes)) == 1, "All columns have to be of the same type"
        # Create and explode an array of (column_name, column_value) structs
        kvs = explode(array([
        struct(lit(c).alias("MES"), col(c).alias("VOLUME")) for c in cols
        ])).alias("kvs")
        return df.select(by + [kvs]).select(by + ["kvs.MES", "kvs.VOLUME"])

    df2 = to_explode(df, ['COMBUSTÍVEL', 'ANO', 'REGIÃO', 'ESTADO', 'UNIDADE'])    

    map_mes = {}
    count = 1
    for i in ['Jan', 'Fev',
        'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']:
        
        map_mes[i] = str(count) if count >= 10 else '0'+ str(count) 
        count += 1
    def mapMes(x):
            return map_mes[x]

    udf_map_mes = udf(lambda x:mapMes(x),StringType() )

    df2 = df2.withColumn("MES",udf_map_mes(col("MES"))).select("*")
    df2 = df2.withColumn("DATA", concat(lit("01/"),col("MES"),lit("/"),col("ANO")))
    df2 = df2.withColumn("year_month", to_date(col("DATA"), "dd/MM/yyyy"))

    df2 = df2.withColumnRenamed('DATA','year_month').withColumnRenamed('COMBUSTÍVEL','product')\
            .withColumnRenamed('VOLUME','volume').withColumnRenamed('UNIDADE','unit').withColumnRenamed('ESTADO','uf')\
            .drop(*('MES', 'REGIÃO', 'ANO'))
            
    df2 = df2.withColumn("created_at", F.current_date())   

    if outputfilename is None:
        outputfilename = output_file_name = "data_{}.parquet".format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    filepath = Path('/opt/airflow/dags/'+ output_file_name)  
    df.write.parquet(filepath)     

sparkLoadExcelToParquet()