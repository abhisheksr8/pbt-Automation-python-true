from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from automationpbttruepython.config.ConfigStore import *
from automationpbttruepython.udfs.UDFs import *
from prophecy.utils import *
from automationpbttruepython.graph import *

def pipeline(spark: SparkSession) -> None:
    df_s3_source_dataset = s3_source_dataset(spark)
    create_lookup_table(spark, df_s3_source_dataset)
    df_select_all_from_in0 = select_all_from_in0(spark, df_s3_source_dataset)
    df_customers_reformatted = customers_reformatted(spark, df_s3_source_dataset)
    df_print_success = print_success(spark, df_customers_reformatted)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("AutomationPBT-truepython")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/AutomationPBTNo-truepython")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/AutomationPBTNo-truepython", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
