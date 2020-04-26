from config.settings import MASTER, APP_NAME, BATCH_INPUT_FILE, BATCH_OUTPUT_FILE
from dependencies.helpers import get_context
from dependencies.queries import BatchQueries
from dependencies.schemas import purchase_schema

def job():
	print('Creating Spark Context.')
	sc, sql_context = get_context(MASTER, APP_NAME)

	print('Reading Data.')
	_ = (sql_context.read.csv(BATCH_INPUT_FILE, schema=purchase_schema, header=True)
			.createOrReplaceTempView('tmp'))

	suspicious_purchases = sql_context.sql(BatchQueries.suspicious_purchases)

	print('Writing Data.')
	_ = (suspicious_purchases
			.repartition(1)
			.write.format("com.databricks.spark.csv")
			.option("header", "true")
			.save(BATCH_OUTPUT_FILE))
