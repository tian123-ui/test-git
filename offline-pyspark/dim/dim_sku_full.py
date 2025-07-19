from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """Initialize SparkSession with Hive support"""
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
    """Write DataFrame to Hive table"""
    jdbcDF.write.mode('overwrite').insertInto(f"{tableName}")

def execute_hive_insert(partition_date: str, tableName):
    """Execute Hive insert operation"""
    spark = get_spark_session()

    # Build the SQL query
    select_sql = f"""
    with
    sku as
        (
            select
                id,
                price,
                sku_name,
                sku_desc,
                weight,
                is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            from ods_sku_info
            where dt='{partition_date}'
        ),
    spu as
        (
            select
                id,
                spu_name
            from ods_spu_info
            where dt='{partition_date}'
        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from ods_base_category3
            where dt='{partition_date}'
        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from ods_base_category2
            where dt='{partition_date}'
        ),
    c1 as
        (
            select
                id,
                name
            from ods_base_category1
            where dt='{partition_date}'
        ),
    tm as
        (
            select
                id,
                tm_name
            from ods_base_trademark
            where dt='{partition_date}'
        ),
    attr as
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            from ods_sku_attr_value
            where dt='{partition_date}'
            group by sku_id
        ),
    sale_attr as
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from ods_sku_sale_attr_value
            where dt='{partition_date}'
            group by sku_id
        )
    insert overwrite table {tableName} partition(ds='{partition_date}')
    select
        sku.id,
        sku.price,
        sku.sku_name,
        sku.sku_desc,
        sku.weight,
        CASE WHEN sku.is_sale = 1 THEN true ELSE false END as is_sale,
        sku.spu_id,
        spu.spu_name,
        sku.category3_id,
        c3.name,
        c3.category2_id,
        c2.name,
        c2.category1_id,
        c1.name,
        sku.tm_id,
        tm.tm_name,
        attr.attrs,
        sale_attr.sale_attrs,
        sku.create_time
    from sku
             left join spu on sku.spu_id=spu.id
             left join c3 on sku.category3_id=c3.id
             left join c2 on c3.category2_id=c2.id
             left join c1 on c2.category1_id=c1.id
             left join tm on sku.tm_id=tm.id
             left join attr on sku.id=attr.sku_id
             left join sale_attr on sku.id=sale_attr.sku_id
    """

    print(f"[INFO] Executing SQL query for partition: {partition_date}")
    spark.sql(select_sql)

    # Verify data
    print(f"[INFO] Verifying data for partition {partition_date}...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE ds='{partition_date}' LIMIT 5")
    verify_df.show()

if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dim_sku_full')