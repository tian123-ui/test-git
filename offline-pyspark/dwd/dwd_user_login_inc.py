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
    user_id,
    date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd') as date_id,  -- 日期ID
    date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time,  -- 登录时间
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from (
         select
             common_uid as user_id,
             common_ch as channel,
             common_ar as province_id,
             common_vc as version_code,
             common_mid as mid_id,
             common_ba as brand,
             common_md as model,
             common_os as operate_system,
             ts,
             row_number() over (partition by common_sid order by ts) as rn  -- 按 session 去重
         from (
                  select
                      -- 解析 JSON 字段（假设日志存在 log 列）
                      get_json_object(log, '$.common.uid') as common_uid,
                      get_json_object(log, '$.common.ch') as common_ch,
                      get_json_object(log, '$.common.ar') as common_ar,
                      get_json_object(log, '$.common.vc') as common_vc,
                      get_json_object(log, '$.common.mid') as common_mid,
                      get_json_object(log, '$.common.ba') as common_ba,
                      get_json_object(log, '$.common.md') as common_md,
                      get_json_object(log, '$.common.os') as common_os,
                      get_json_object(log, '$.ts') as ts,  -- 解析时间戳
                      get_json_object(log, '$.common.sid') as common_sid,  -- 解析 session_id
                      get_json_object(log, '$.page') as page  -- 解析 page 字段（过滤用）
                  from ods_z_log
                  where dt = '20250701'
                    -- 过滤条件：page 非空 + 用户 ID 非空
                    and get_json_object(log, '$.page') is not null
                    and get_json_object(log, '$.common.uid') is not null
              ) t1
     ) t2
where rn = 1;
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
    execute_hive_insert(target_date, 'dwd_user_login_inc')
