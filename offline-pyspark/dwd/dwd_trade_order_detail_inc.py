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
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.create_time,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods_order_detail data
        where `dt` = '20250701'
    ) od
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods_order_info data
        where `dt` = '20250701'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods_order_detail_activity data
        where `dt` = '20250701'

    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods_order_detail_coupon data
        where `dt` = '20250701'

    ) cou
    on od.id = cou.order_detail_id;
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
    execute_hive_insert(target_date, 'dwd_trade_order_detail_inc')
