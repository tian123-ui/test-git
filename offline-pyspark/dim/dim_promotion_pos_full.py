from pyspark.sql import SparkSession


def get_spark_session():
    """初始化SparkSession并配置Hive连接"""
    spark = SparkSession.builder \
        .appName("PromotionPosETL") \
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


def execute_promotion_pos_etl(partition_date: str):
    """执行促销位点维度ETL"""
    spark = get_spark_session()

    # 创建外部表（如果不存在）
    spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS dim_promotion_pos_full
    (
        `id` STRING COMMENT '促销位点ID',
        `pos_location` STRING COMMENT '位点位置',
        `pos_type` STRING COMMENT '位点类型',
        `promotion_type` STRING COMMENT '促销类型',
        `create_time` STRING COMMENT '创建时间',
        `operate_time` STRING COMMENT '操作时间'
    )
    COMMENT '促销位点维度表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dim/dim_promotion_pos_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy')
    """)

    # 执行数据加载
    insert_sql = f"""
    INSERT OVERWRITE TABLE dim_promotion_pos_full PARTITION(ds='{partition_date}')
    SELECT 
        `id`,
        `pos_location`,
        `pos_type`,
        `promotion_type`,
        `create_time`,
        `operate_time`
    FROM ods_promotion_pos
    WHERE dt='{partition_date}'
    """

    print(f"正在加载数据到dim_promotion_pos_full，分区ds={partition_date}")
    spark.sql(insert_sql)

    # 验证数据
    print("数据验证：")
    spark.sql(f"SELECT * FROM dim_promotion_pos_full WHERE ds='{partition_date}' LIMIT 5").show()


if __name__ == "__main__":
    execute_promotion_pos_etl('20250701')