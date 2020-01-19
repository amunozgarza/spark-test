import findspark
findspark.init("/opt/spark")
import random
import pandas as pd

class diccionario():
	def __init__(self):
		super(diccionario, self).__init__()
		self.productos = {}
	def add_delito(self, name, value):
		self.productos[name] = value
	def update_value(self, name):
		self.productos[name] += 1
	def return_productos(self):
		return self.productos

from pyspark import SparkContext
sc = SparkContext(appName="EstimatePi")
'''
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1
NUM_SAMPLES = 1000000
count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
'''
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from collections import Counter
sqlContext = SQLContext(sc)
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', 
	inferschema='true').load("/media/andres/OS/Users/Andres/Documents/OPI_prueba/all_data.csv")
cat = df.select('categoria').distinct().rdd.map(lambda r: r[0]).collect()
print("Numero de Categorias: ", len(cat))

print(df.show())
df.printSchema()
print(df.select('producto').distinct().rdd.map(lambda r: r[0]).collect())
#input()
print("Numero de registros: ", df.count())
print("Numero de Categorias: ", len(df.columns))
supers = df.select('cadenaComercial').distinct().rdd.map(lambda r: r[0]).collect()
print("Numero de cadenas siendo monitoreadas: ", len(supers))
#df.describe([]).show()
entidad = {}

counts = df.groupBy(['estado', 'producto']).count().alias('counts')
counts.show()
supers = counts.select('estado').distinct().rdd.map(lambda r: r[0]).collect()
#print(supers)
counts = counts.sort(col("count").desc())
panda = counts.toPandas()
#print(panda)
#print(df.select('').toPandas()

for key in supers:
	productos = diccionario()
	for i in range(len(panda["producto"])):
		if panda["estado"][i] == key:
			#print((panda["delito"][i], panda["count"][i]))
			productos.add_delito(panda["producto"][i], panda["count"][i])
			break
	entidad[key] = productos.return_productos()
	#print(entidad)
	#input()
print(entidad)


cadena = {}
product = {}

counts = df.groupBy(['cadenaComercial', 'producto']).count().alias('counts')
counts.show()
supers = df.select('cadenaComercial').distinct().rdd.map(lambda r: r[0]).collect()
prod = supers
#print(supers)
#counts = counts.sort(col("count").desc())
panda = counts.toPandas()
#print(panda)
#print(df.select('').toPandas()

for key in prod:
	#productos = diccionario()
	c = 0
	for i in range(len(panda["producto"])):
		if panda["cadenaComercial"][i] == key:
			if panda["producto"][i] not in product:
				product[panda["producto"][i]] = 0
				c += 1
			#print((panda["delito"][i], panda["count"][i]))
			#productos.add_delito(panda["cadenaComercial"][i], panda["count"][i])
			#break
	cadena[key] = c
	#print(entidad)
	#input()
max_value = max(cadena.values())  # maximum value
max_keys = [k for k, v in cadena.items() if v == max_value]
print("Cadena con la mayor variedad de productos monitoreados: ", max_keys, max_value)


print(" ")
cols = ["producto", "cadenaComercial", "municipio", "estado", "fechaRegistro", "precio"]
for col in df.columns:
	print(col, "\t", "with null values: ", df.filter(df[col].isNull()).count())
sc.stop()