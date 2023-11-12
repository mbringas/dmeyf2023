import pandas as pd
base="/home/maubringas/buckets/b1/exp/"
experiments=["baseline_undersampling03_100seeds"]#,"baseline_undersampling03_100seeds","baseline_undersampling03_100seeds","baseline_undersampling03_100seeds","baseline_undersampling03_100seeds"]
gains=[9000]
for e in experiments:
	for g in gains:
		for i in range(1,51):
			print(i)
			currfile=pd.read_csv(base+e+"/baseline_undersampling03_100seeds_"+str(i)+"_"+str(g)+".csv")
			print(currfile)				


