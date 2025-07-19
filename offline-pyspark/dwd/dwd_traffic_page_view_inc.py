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
            common.province_id,
            common.brand,
            common.channel,
            common.is_new,
            common.model,
            common.mid_id,
            common.operate_system,
            common.user_id,
            common.version_code,
            page_data.item as page_item,
            page_data.item_type as page_item_type,
            page_data.last_page_id,
            page_data.page_id,
            page_data.from_pos_id,
            page_data.from_pos_seq,
            page_data.refer_id,
            date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
            date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
            common.session_id,
            cast(page_data.during_time as bigint) as during_time  -- 关键修改：将字符串转换为bigint
        from (
                 select
                     get_json_object(log, '$.common') as common_json,
                     get_json_object(log, '$.page') as page_json,
                     get_json_object(log, '$.ts') as ts
                 from ods_z_log
                 where dt='20250701'  -- 使用动态日期
             ) base
                 lateral view json_tuple(common_json, 'ar', 'ba', 'ch', 'is_new', 'md', 'mid', 'os', 'uid', 'vc', 'sid') common as province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, session_id
                 lateral view json_tuple(page_json, 'item', 'item_type', 'last_page_id', 'page_id', 'from_pos_id', 'from_pos_seq', 'refer_id', 'during_time') page_data as item, item_type, last_page_id, page_id, from_pos_id, from_pos_seq, refer_id, during_time
        where page_json is not null;
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
    execute_hive_insert(target_date, 'dwd_traffic_page_view_inc')
