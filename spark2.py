import findspark
findspark.init("/opt/spark")
import random
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class diccionario():
	def __init__(self):
		super(diccionario, self).__init__()
		self.productos = {}
	def add_delito(self, name):
		self.productos[name] = 0
	def update_value(self, name, value, times):
		if name not in self.productos:
			self.add_delito(name)
		self.productos[name] += value/times
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

municipios = df.select('municipio').distinct().rdd.map(lambda r: r[0]).collect()
data_1 = df.select(['municipio', 'producto']).toPandas()
data_2 = df.select(['precio', 'fechaRegistro']).toPandas()
canasta_basica = ["FRIJOL", "ARROZ", "CARNE DE POLLO", "CAFE SOLUBLE", "LECHE PASTEURIZADA", 
"DETERGENTE P/ROPA Y TRASTES", "TORTILLA DE MAIZ", "PAPEL HIGIENICO", "PLATANO", "PAN BLANCO BOLILLO"]
canastas = {}

for city in municipios:
	basica = diccionario()
	c = 0
	for i, item in enumerate(canasta_basica):
		if (data_1['producto'][i] == item) and (data_1['municipio'][i] == city):
			if (data_2['fechaRegistro'][i][0:4] == "2015"):
				c += 1
				basica.update_value(item, data_2['precio'][i], c)
	average = 0
	for key, value in basica.items():
		average += value
	canastas[city] = average

max_value = max(canastas.values())  # maximum value
max_keys = [k for k, v in canastas.items() if v == max_value]
min_value = min(canastas.values())  # maximum value
min_keys = [k for k, v in canastas.items() if v == min_value]

print("Ciudad mas cara: ", max_keys, max_value, " Ciudad mas barata: ", min_keys, min_value)

mes_caro = {}
valores = {}
meses = np.arange(1, 13)
estados = df.select('estado').distinct().rdd.map(lambda r: r[0]).collect()
data_3 = df.select(['estado'])
municipios = 0
for estado in estados:
	meses_temp = diccionario()
	for mes in meses:
		basica = diccionario()
		c = 0
		for i, item in enumerate(canasta_basica):
			if (data_1['producto'][i] == item) and (data_3['estado'][i] == estado):
				if (int(data_2['fechaRegistro'][5:7]) == mes) and (data_2['fechaRegistro'][i][0:4] == "2015"):
					c +=1
					basica.update_value(item, data_2['precio'][i], c)
		average = 0
		for key, value in basica.items():
			average += value
		meses_temp[mes] = average
	
	max_value = max(meses_temp.values())  # maximum value
	max_keys = [k for k, v in meses_temp.items() if v == max_value]
	mes_caro[estado] = max_keys[0]
	valores[estado] = max_value

max_value = max(mes_caro.values())  # maximum value
max_keys = [k for k, v in mes_caro.items() if v == max_value]

print("Estado mas caro y mes mas caro: ", max_keys[0], max_value, valores[max_keys[0]])

anos = ["2011", "2012", "2013", "2014", "2015"]
avg = diccionario()

for year in anos:
	c = 0
	for i, date in enumerate(data_2['fechaRegistro']):
		for j, item in enumerate(canasta_basica):
			if (date[0:4] == year) and (data_1['producto'][i] == item):
				c += 1
				avg.update_value(year, data_2['precio'], c)

graph = []
for key, value in avg.items():
	graph.append(value)
plt.plot(graph)
plt.show()

sc.stop()




			
				



