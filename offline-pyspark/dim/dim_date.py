from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.ignoreMissingPartitions", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS gmall")
    spark.sql("USE gmall")

    return spark

def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    # 移除ds列后再写入
    jdbcDF.drop("ds").write.mode('append').insertInto(f"{tableName}")

def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive表"""
    spark = get_spark_session()

    # 构建动态SQL查询
    select_sql1 = f"""
    select * from tmp_dim_date_info;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段ds（仅用于演示，实际不写入目标表）
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)

    # 写入数据（会自动移除ds列）
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} LIMIT 5")
    verify_df.show()

if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dim_date')