# Script BO baseline para Experimento colaborativo

# se entrena con clase_binaria2  POS =  { BAJA+1, BAJA+2 }
# Optimizacion Bayesiana de hiperparametros de  lightgbm,

# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("rlist")


dataset <- "/home/maubringas/buckets/b1/datasets/colaborativos_withTargets.csv.gz"

dataset <- fread(dataset)

dataset[ foto_mes == 202107,clase01 := ifelse(clase_ternaria == "BAJA+2", 1L, 0L)]

dataset=dataset[,.(numero_de_cliente,clase_ternaria,foto_mes)]
fwrite(dataset,"data_test.csv")
# libero espacio
gc()

