from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from automationpbttruepython.config.ConfigStore import *
from automationpbttruepython.functions import *

def create_lookup_test(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''customer_id''', '''email''']
    valueColumns = ['''country_code''']
    createLookup("LookupTest", in0, spark, keyColumns, valueColumns)
