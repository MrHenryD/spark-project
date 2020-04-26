from pyspark.sql.types import *

purchase_schema = StructType([
    StructField('user', LongType(), nullable=False),
    StructField('timestamp', LongType(), nullable=False),
    StructField('price', FloatType(), nullable=False),
    StructField('suspicious', ByteType(), nullable=False),
])

