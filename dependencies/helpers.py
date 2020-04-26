import csv
from datetime import datetime
from random import randint

from pyspark import SparkContext, SQLContext

def get_context(master, app_name):
	sc = SparkContext(master, app_name)
	sql_context = SQLContext(sc)
	return sc, sql_context

def generate_batch_data(csv_path, n_rows=10000):
	def generate_random(lower_limit, upper_limit):
		return randint(lower_limit, upper_limit)

	columns = ('user', 'timestamp', 'price', 'suspicious')
	
	epoch_start = datetime(2012,4,1,0,0).timestamp()
	epoch_end = datetime(2014,4,1,0,0).timestamp()

	rows = [columns] + [(generate_random(1, 10000), 
			 generate_random(epoch_start, epoch_end),
			 generate_random(30, 100),
			 generate_random(0, 1)) for _ in range(n_rows)]

	with open(csv_path, 'w+', newline='', encoding='utf-8') as outfile:
		writer = csv.writer(outfile)
		writer.writerows(rows)

def generate_stream_data():
	pass

if __name__ == '__main__':
	_ = generate_purchase_data('purchase_data.csv')
