from collections import Counter
import pandas as pd

class Alcaldia():
	def __init__(self, total):
		super(Alcaldia, self).__init__()
		self.delitos = {}
		self.porcentajes = {}
		self.total = total
	def add_delito(self, name):
		self.delitos[name] = 1
	def update_value(self, name):
		self.delitos[name] += 1
	def porcentaje(self):
		for key, value in self.delitos.items():
			self.porcentajes[key] = value/self.total
		return self.porcentajes
	def return_delitos(self):
		return self.delitos
train_set = pd.read_csv("/media/andres/OS/Users/Andres/Documents/OPI_prueba/carpetas-de-investigacion-pgj-de-la-ciudad-de-mexico.csv", sep=',')

total = Counter(train_set["alcaldia_hechos"])
#print(total)
#input()
indices = {}
count = 0
for key, value in total.items():
	idxs = []
	for i, x in enumerate(train_set["alcaldia_hechos"]):
		if x == key:
			idxs.append(i)
	indices[key] = idxs
	count += 1
	if count == 70:
		break

alcaldias = {}
keys = []
count = 0
for key, value in total.items():
	print(key, total[key])
	a = Alcaldia(total[key])
	for i in indices[key]:
		dict_ = a.return_delitos()
		delito = train_set["delito"][i]
		if delito not in dict_:
			#print(delito)
			a.add_delito(delito)
		else:
			#print(delito)
			a.update_value(delito)
	percentage = a.porcentaje()
	
	if len(indices[key]) > 20000:
		top10 = []
		counter = 0
		for k in sorted(percentage, key=percentage.get, reverse=True):
			top10.append((k, percentage[k]))
			if counter == 39:
				break
			counter += 1
		#print(top10)
		alcaldias[key] = top10
		keys.append(key)
		count += 1
		if count == 13:
			break
		print(count)
	#input()
best = {}
for key, value in alcaldias.items():
	print(key)
	#input()
	#print(alcaldias[key])
	for i in range(len(alcaldias[key])):
		b = 0
		better = 0
		for name in keys:
			for j in range(len(alcaldias[name])):
				#print(alcaldias[key][i][0])
				#print(alcaldias[name][j][0] ,alcaldias[key][i][0], b)
				if alcaldias[name][j][0] == alcaldias[key][i][0]:
					b += 1
			#if b == 0:
			#	print('a')
			#	best[key] = value[i]
			
			#print(alcaldias[name][i][1] ,alcaldias[key][i][1], better)
			diff = alcaldias[name][i][1] - alcaldias[key][i][1]
			if diff < 0:
				diff = -1.0 * diff
			#print(diff,  alcaldias[name][i][1]*0.20)
			if diff >= alcaldias[name][i][1]*0.15:
				better += 1
			if better > 8:
				best[key] = value[i]
			#print(best)
			#input()
			
print(best)
input()