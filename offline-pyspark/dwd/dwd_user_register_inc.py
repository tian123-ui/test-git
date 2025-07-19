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
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName: str):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询
    select_sql1 = f"""
 select
    ui.user_id,
    date_format(ui.create_time,'yyyy-MM-dd') date_id,
    ui.create_time,
    log.channel,
    log.province_id,
    log.version_code,
    log.mid_id,
    log.brand,
    log.model,
    log.operate_system
from
    (
        select
            data.id as user_id,
            data.create_time
        from ods_user_info data
        where dt='20250701'

    ) ui
        left join
    (
        select
            get_json_object(log, '$.common.ar') as province_id,  -- 修改此处，使用正确的列名log
            get_json_object(log, '$.common.ba') as brand,
            get_json_object(log, '$.common.ch') as channel,
            get_json_object(log, '$.common.md') as model,
            get_json_object(log, '$.common.mid') as mid_id,
            get_json_object(log, '$.common.os') as operate_system,
            get_json_object(log, '$.common.uid') as user_id,
            get_json_object(log, '$.common.vc') as version_code
        from ods_z_log
        where dt='20250701'
          and get_json_object(log, '$.page.page_id') = 'register'  -- 修改此处，使用正确的列名log
          and get_json_object(log, '$.common.uid') is not null
    ) log
    on ui.user_id = log.user_id;
        """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段ds
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)
    print(f"[INFO] DataFrame列数: {len(df_with_partition.columns)}")  # 应该显示7列

    # 写入数据
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE ds='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dwd_user_register_inc')
