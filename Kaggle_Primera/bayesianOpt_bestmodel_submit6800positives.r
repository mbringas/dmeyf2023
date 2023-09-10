# Arbol elemental con libreria  rpart
# Debe tener instaladas las librerias  data.table  ,  rpart  y  rpart.plot

# cargo las librerias que necesito
require("data.table")
require("rpart")
require("rpart.plot")

# Aqui se debe poner la carpeta de la materia de SU computadora local
setwd("/home/mauro/Downloads/DMEyF/dmeyf2023/different_eventNumber_20230904/") # Establezco el Working Directory

semillas <- c(369821, 369827, 369829, 369833, 369841)

# Cargamos el dataset
dataset <- fread('../calculate_target_20230815/competencia_01_withTargs.csv')

# Nos quedamos solo con el 202101
#dataset <- dataset[foto_mes == 202103]
# Creamos una clase binaria
dataset[, clase_binaria := ifelse(
  clase_ternaria == "BAJA+2",
  "evento",
  "noevento"
)]
# Borramos el target viejo
dataset[, clase_ternaria := NULL]

# Seteamos nuestra primera semilla
set.seed(semillas[1])
dtrain <- dataset[foto_mes == 202103] # defino donde voy a entrenar
dapply <- dataset[foto_mes == 202105] # defino donde voy a aplicar el modelo

hiperparams_grid_search <- list(c(-1,407, 1297,6,65986667,"m1"))

#dir.create("./exp/")
for (hpars in hiperparams_grid_search){
 
  # genero el modelo,  aqui se construye el arbol
  # quiero predecir clase_ternaria a partir de el resto de las variables
  print(hpars[4])
  modelo <- rpart(
          formula = "clase_binaria ~ .",
          data = dtrain, # los datos donde voy a entrenar
          xval = 0,
          cp = hpars[1], # esto significa no limitar la complejidad de los splits
          minsplit = hpars[3], # minima cantidad de registros para que se haga el split
          minbucket = hpars[2], # tamaño minimo de una hoja
          maxdepth = 6
  ) # profundidad maxima del arbol
  
  
  # grafico el arbol
  prp(modelo,
          extra = 101, digits = -5,
          branch = 1, type = 4, varlen = 0, faclen = 0
  )
  
  
  # aplico el modelo a los datos nuevos
  prediccion <- predict(
          object = modelo,
          newdata = dapply,
          type = "prob"
  )
  
  # prediccion es una matriz con TRES columnas,
  # llamadas "BAJA+1", "BAJA+2"  y "CONTINUA"
  # cada columna es el vector de probabilidades
  
  # agrego a dapply una columna nueva que es la probabilidad de BAJA+2
  dapply[, prob_baja2 := prediccion[, "evento"]]

  sorted_dapply <- dapply[order(prob_baja2,decreasing=TRUE)]  
  for (to_send in c(6800)){
     # solo le envio estimulo a los registros
      #  con probabilidad de BAJA+2 mayor  a  1/40
      sorted_dapply$Predicted <- 0
      sorted_dapply$Predicted[1:to_send]=1
      print(sum(sorted_dapply$Predicted))
      to_send <- sorted_dapply[order(numero_de_cliente)]  
      # genero el archivo para Kaggle
      # primero creo la carpeta donde va el experimento
    #  
    #  dir.create("./exp/pred_"+hpars[6]+"/")
      
      # solo los campos para Kaggle
      fwrite(to_send[, list(numero_de_cliente, Predicted)],
              file = paste("./exp/pred_",hpars[6],"_submitted_",sum(to_send$Predicted),".csv",sep=""),
              sep = ","
  )
  }
}
