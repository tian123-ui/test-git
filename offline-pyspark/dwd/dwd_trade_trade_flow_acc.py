from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col


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
    try:
        spark = get_spark_session()

        # 将日期转换为yyyymmdd格式用于源表分区过滤
        source_date = partition_date.replace('-', '')

        # 构建动态SQL查询
        select_sql1 = f"""
        select
            oi.id,
            oi.user_id,
            oi.province_id,
            oi.create_time,  -- 添加原始时间戳列
            pi.callback_time,  -- 添加原始回调时间戳列
            log.finish_time,  -- 添加原始完成时间戳列
            date_format(oi.create_time, 'yyyy-MM-dd') as create_date,  -- 格式化日期列
            date_format(pi.callback_time, 'yyyy-MM-dd') as callback_date,  -- 格式化日期列
            date_format(log.finish_time, 'yyyy-MM-dd') as finish_date,  -- 格式化日期列
            oi.original_total_amount,
            oi.activity_reduce_amount,
            oi.coupon_reduce_amount,
            oi.total_amount,
            nvl(pi.payment_amount, 0.0) as payment_amount
        from
            (
                select
                    data.id,
                    data.user_id,
                    data.province_id,
                    data.create_time,
                    data.original_total_amount,
                    data.activity_reduce_amount,
                    data.coupon_reduce_amount,
                    data.total_amount
                from ods_order_info data
                where dt='{source_date}'
            )oi
                left join
            (
                select
                    data.order_id,
                    data.callback_time,
                    data.total_amount payment_amount
                from ods_payment_info data
                where dt='{source_date}'
                  and data.payment_status='1602'
            )pi
            on oi.id=pi.order_id
                left join
            (
                select
                    data.order_id,
                    data.create_time finish_time
                from ods_order_status_log data
                where dt='{source_date}'
                  and data.order_status='1004'
            )log
            on oi.id=log.order_id;
            """

        print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
        df1 = spark.sql(select_sql1)

        # 添加分区字段ds
        df_with_partition = df1.withColumn("ds", lit(partition_date))

        print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
        df_with_partition.show(5)
        print(f"[INFO] DataFrame列数: {len(df_with_partition.columns)}")


        # 写入数据
        select_to_hive(df_with_partition, tableName)

        # 验证数据
        print(f"[INFO] 验证分区 {partition_date} 的数据...")
        verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE ds='{partition_date}' LIMIT 5")
        verify_df.show()

        print(f"[INFO] 分区 {partition_date} 数据处理完成")

    except Exception as e:
        print(f"[ERROR] 处理分区 {partition_date} 时发生错误: {str(e)}")
        raise


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dwd_trade_trade_flow_acc')