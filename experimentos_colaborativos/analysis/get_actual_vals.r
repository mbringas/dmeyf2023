# Script BO baseline para Experimento colaborativo

# se entrena con clase_binaria2  POS =  { BAJA+1, BAJA+2 }
# Optimizacion Bayesiana de hiperparametros de  lightgbm,

# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("rlist")


dataset <- "/home/maubringas/buckets/b1/datasets/colaborativos_features.csv.gz"

# los meses en los que vamos a entrenar
#  mucha magia emerger de esta eleccion
testing <- c(202107)

# cargo el dataset donde voy a entrenar el modelo
dataset <- fread(dataset)

# agrego lag1, lag3 y lag6
all_columns <-  c("numero_de_cliente", "clase_ternaria")

# defino los datos de testing
dataset[ foto_mes == 202107,clase01 := ifelse(clase_ternaria == "BAJA+2", 1L, 0L)]

dataset[,.(numero_de_cliente,clase01)
fwrite(dataset,"data_test.csv")
# libero espacio
gc()

cat("\n\nLa optimizacion Bayesiana ha terminado\n")
