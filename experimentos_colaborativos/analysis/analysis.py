import numpy as np
import pandas as pd
base="/home/maubringas/buckets/b1/exp/"
experiments=["baseline_undersampling03_100seeds"]#,"baseline_undersampling03_100seeds","baseline_undersampling03_100seeds","baseline_undersampling03_100seeds","baseline_undersampling03_100seeds"]
gains=[9000]
test=pd.read_csv("/home/maubringas/buckets/b1/datasets/data_test.csv")
test.sort_values(by="nrocli",inplace=True)
for e in experiments:
	for g in gains:
		for i in range(1,51):
			currfile=pd.read_csv(base+e+"/"+e+"_"+str(i)+"_"+str(g)+".csv")
			currfile.sort_values(by="numero_de_cliente",inplace=True)
			table=currfile.set_index('numero_de_cliente').join(test.set_index('nrocli'))
#pd.concat([currfile,test],axis=1)
			table["gan"]=0
			cond1= np.logical_and(table["Predicted"]==1, table["test_val"]==1)
			table.loc[cond1,"gan"]=273000
			cond2= np.logical_and(table["Predicted"]==1, table["test_val"]==0)
			table.loc[cond2,"gan"]=-7000
			print(sum(table["test_val"]==1))
			print(sum(cond1),sum(cond2))
			print(table.gan.sum())				


