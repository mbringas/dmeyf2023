# para correr el Google Cloud
#   8 vCPU
#  16 GB memoria RAM

# LR,feature fraction,min_data_in_leaves,num_leaves,nombre, num_iterations

best_model<-c(c(0.03,0.325,101,480,"BO3_model1",1922) )

semilla <- 369821

# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("lightgbm")


# defino los parametros de la corrida, en una lista, la variable global  PARAM
#  muy pronto esto se leera desde un archivo formato .yaml
PARAM <- list()
PARAM$experimento <-"EXPCOL_mejormodelo_50semillas_0.5"

PARAM$input$dataset <- "./datasets/colaborativos_features.csv.gz"

# meses donde se entrena el modelo
PARAM$input$training <- c(202012,202101, 202102, 202103, 202104, 202105)
PARAM$input$future <- c(202106) # meses donde se aplica el modelo

PARAM$finalmodel$num_iterations <- best_model[6]#4928
PARAM$finalmodel$learning_rate <- best_model[1]#0.0189943331895954
PARAM$finalmodel$feature_fraction <- best_model[2]#0.892623977897483
PARAM$finalmodel$min_data_in_leaf best_model[3]<- #785
PARAM$finalmodel$num_leaves <- best_modeli[4] #666


PARAM$finalmodel$max_bin <- 31

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui empieza el programa
setwd("~/buckets/b1")

# cargo el dataset donde voy a entrenar
dataset <- fread(PARAM$input$dataset, stringsAsFactors = TRUE)


#--------------------------------------

# paso la clase a binaria que tome valores {0,1}  enteros
# set trabaja con la clase  POS = { BAJA+1, BAJA+2 }
# esta estrategia es MUY importante
dataset[, clase01 := ifelse(clase_ternaria %in% c("BAJA+2", "BAJA+1"), 1L, 0L)]

#--------------------------------------

# los campos que se van a utilizar
campos_buenos <- setdiff(colnames(dataset), c("clase_ternaria", "clase01"))

#--------------------------------------


# establezco donde entreno
dataset[, train := 0L]
dataset[foto_mes %in% PARAM$input$training, train := 1L]

#--------------------------------------
# creo las carpetas donde van los resultados
# creo la carpeta donde va el experimento
dir.create("./exp/", showWarnings = FALSE)
dir.create(paste0("./exp/", PARAM$experimento, "/"), showWarnings = FALSE)

# Establezco el Working Directory DEL EXPERIMENTO
setwd(paste0("./exp/", PARAM$experimento, "/"))

for (i in 1:50){     
     dir.create(paste0("./exp/", PARAM$experimento, "/", str(i) ,"/"), showWarnings = FALSE)
     setwd(paste0("./exp/", PARAM$experimento, "/", str(i) ,"/"))

     # dejo los datos en el formato que necesita LightGBM
     dtrain <- lgb.Dataset(
       data = data.matrix(dataset[train == 1L, campos_buenos, with = FALSE]),
       label = dataset[train == 1L, clase01]
     )
     
     # genero el modelo
     # estos hiperparametros  salieron de una laaarga Optmizacion Bayesiana
     modelo <- lgb.train(
       data = dtrain,
       param = list(
         objective = "binary",
         max_bin = PARAM$finalmodel$max_bin,
         learning_rate = PARAM$finalmodel$learning_rate,
         num_iterations = PARAM$finalmodel$num_iterations,
         num_leaves = PARAM$finalmodel$num_leaves,
         min_data_in_leaf = PARAM$finalmodel$min_data_in_leaf,
         feature_fraction = PARAM$finalmodel$feature_fraction,
         seed = semilla
       )
     )
     
     #--------------------------------------
     # ahora imprimo la importancia de variables
     tb_importancia <- as.data.table(lgb.importance(modelo))
     archivo_importancia <- "impo.txt"
     
     fwrite(tb_importancia,
       file = archivo_importancia,
       sep = "\t"
     )
     
     #--------------------------------------
     
     
     # aplico el modelo a los datos sin clase
     dapply <- dataset[foto_mes == PARAM$input$future]
     
     # aplico el modelo a los datos nuevos
     prediccion <- predict(
       modelo,
       data.matrix(dapply[, campos_buenos, with = FALSE])
     )
     
     # genero la tabla de entrega
     tb_entrega <- dapply[, list(numero_de_cliente, foto_mes)]
     tb_entrega[, prob := prediccion]
     
     # grabo las probabilidad del modelo
     fwrite(tb_entrega,
       file = "prediccion.txt",
       sep = "\t"
     )
     
     # ordeno por probabilidad descendente
     setorder(tb_entrega, -prob)
     
     
     # genero archivos con los  "envios" mejores
     # deben subirse "inteligentemente" a Kaggle para no malgastar submits
     # si la palabra inteligentemente no le significa nada aun
     # suba TODOS los archivos a Kaggle
     # espera a la siguiente clase sincronica en donde el tema sera explicado
     
     cortes <- seq(8000, 13000, by = 500)
     for (envios in cortes) {
       tb_entrega[, Predicted := 0L]
       tb_entrega[1:envios, Predicted := 1L]
     
       fwrite(tb_entrega[, list(numero_de_cliente, Predicted)],
         file = paste0(PARAM$experimento, "_", envios, ".csv"),
         sep = ","
       )
     }
     cat("\n\nLa generacion de los archivos para Kaggle ha terminado\n")
     setwd(paste0("./exp/", PARAM$experimento, "/"))

}
cat("\n\nTAMOOO\n")

