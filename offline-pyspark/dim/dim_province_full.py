from pyspark.sql import SparkSession


def get_spark_session():
    """初始化SparkSession并配置Hive连接"""
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


def execute_hive_insert(partition_date: str, tableName: str):
    """执行Hive插入操作"""
    spark = get_spark_session()

    # 方法一：直接使用SQL插入（推荐）
    insert_sql = f"""
    INSERT OVERWRITE TABLE {tableName} PARTITION(ds='{partition_date}')
    SELECT 
        province.id,
        province.name,
        province.area_code,
        province.iso_code,
        province.iso_3166_2,
        province.region_id,
        region.region_name
    FROM (
        SELECT 
            id, name, region_id, 
            area_code, iso_code, iso_3166_2
        FROM ods_base_province 
        WHERE dt='{partition_date}'
    ) province
    LEFT JOIN (
        SELECT id, region_name 
        FROM ods_base_region 
        WHERE dt='{partition_date}'
    ) region ON province.region_id = region.id
    """

    print(f"[INFO] 开始执行SQL插入，目标表: {tableName}，分区: {partition_date}")
    spark.sql(insert_sql)

    # 验证数据
    print(f"[INFO] 验证分区数据...")
    spark.sql(f"SELECT * FROM {tableName} WHERE ds='{partition_date}' LIMIT 5").show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dim_province_full')