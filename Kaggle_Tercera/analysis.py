import numpy as np
import pandas as pd
import glob, os 
base="/home/maubringas/buckets/b1/exp/"
experiments=["Kaggle_Tercera_Semillerio_modelo1"]
gains=[9000]
test.sort_values(by="nrocli",inplace=True)
for e in experiments:
	for g in gains:
		for i in range(1,10):
			currfile=pd.read_csv(base+e+"/"+e+"_"+str(i)+"_"+str(g)+".csv")
			currfile.sort_values(by="numero_de_cliente",inplace=True)
			table=currfile.set_index('numero_de_cliente').join(test.set_index('nrocli'))
#pd.concat([currfile,test],axis=1)
			table["gan"]=0
			cond1= np.logical_and(table["Predicted"]==1, table["resul_test"]==1)
			table.loc[cond1,"gan"]=273000
			cond2= np.logical_and(table["Predicted"]==1, table["resul_test"]==0)
			table.loc[cond2,"gan"]=-7000
			print(e,g,i,table.gan.sum())				


