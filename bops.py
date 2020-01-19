import pandas as pd
import numpy as np

data = pd.read_csv("/media/andres/OS/Users/Andres/Documents/OPI_prueba/BOPS_case/BOPS_case/bops_bm.csv", sep=',')
pre = 0
post = 0
pre_c = 0
post_c = 0
for i in range(len(data)):
	#print(0+np.nan)
	#input()
	if data['usa'][i] == 1:
		if (data["year"][i] == 2011): #and (data["month"][i] <= 10):
			if (data['week'][i] <= 42):
				#print("Pre: ", data["month"][i])
				pre += int(data[' sales '][i].replace(',', ''))
		#elif (data["year"][i] == 2011) and (data["month"][i] >= 10):
			elif (data['week'][i] >= 43):
				#print("Post: ", data["month"][i])
				post += int(data[' sales '][i].replace(',', ''))
		elif data["year"][i] == 2012:
			#print("Next: ", data["month"][i])
			post += int(data[' sales '][i].replace(',', ''))
	elif data['usa'][i] == 0:
		if (data["year"][i] == 2011): #and (data["month"][i] <= 10):
			if (data['week'][i] <= 42):
				#print("Pre: ", data["month"][i])
				pre_c += int(data[' sales '][i].replace(',', ''))
		#elif (data["year"][i] == 2011) and (data["month"][i] >= 10):
			elif (data['week'][i] >= 43):
				#print("Post: ", data["month"][i])
				post_c += int(data[' sales '][i].replace(',', ''))
		elif data["year"][i] == 2012:
			#print("Next: ", data["month"][i])
			post_c += int(data[' sales '][i].replace(',', ''))
print("BOPS BM: ", pre - post)
print("USA BM Pre: ", pre, " USA BM Post: " , post, "Percentage: ", (1.0 - (post/pre))*100)
print("CAN BM Pre: ", pre_c, " CAN BM Post: ", post_c, "Percentage: ", (1.0 - (post_c/pre_c))*100)

data = pd.read_csv("/media/andres/OS/Users/Andres/Documents/OPI_prueba/BOPS_case/BOPS_case/bops_online.csv", sep=',')
pre_o = 0
post_o = 0
close_pre = 0
close_post = 0
far_pre = 0
far_post = 0
for i in range(len(data)):
	if (data["year"][i] == 2011):#and (data["month"][i] <= 10):
		if (data['week'][i] <= 42):
			
			pre_o += int(data[' sales '][i].replace(',', ''))
			if (data['close'][i] == 0):
				far_pre += int(data[' sales '][i].replace(',', ''))
			else:
				close_pre += int(data[' sales '][i].replace(',', ''))
			
	#elif (data["year"][i] == 2011):# and (data["month"][i] >= 10):
		elif (data['week'][i] >= 43):
			
			post_o += int(data[' sales '][i].replace(',', ''))
			if (data['close'][i] == 0):
				far_post += int(data[' sales '][i].replace(',', ''))
			else:
				close_post += int(data[' sales '][i].replace(',', ''))
			
	elif data["year"][i] == 2012:
		
		post_o += int(data[' sales '][i].replace(',', ''))
		if (data['close'][i] == 0):
			far_post += int(data[' sales '][i].replace(',', ''))
		else:
			close_post += int(data[' sales '][i].replace(',', ''))
print("BOPS Online: ", pre_o -  post_o)

total_pre = pre + pre_o
total_post = post + post_o
print("Total Pre = ", total_pre)
print("Total Post = ", total_post)
print("Total loss or gain = ", (total_pre - total_post), " Percentage: ", (1.0 - (total_post/total_pre))*100)
print("Change far locations =  ", far_pre - far_post, " Pre: ", far_pre, " Post: ", far_post, "Percentage: ", (1.0 - (far_post/far_pre))*100)
print("Change close locations =  ", close_pre - close_post, " Pre: ", close_pre, " Post: ", close_post, "Percentage: ", (1.0 - (close_post/close_pre))*100)
