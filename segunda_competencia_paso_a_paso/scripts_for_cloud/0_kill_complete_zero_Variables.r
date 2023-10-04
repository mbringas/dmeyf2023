# Este script genera graficos que muestra que para algunos meses,
#  ciertas variables #  fueron pisadas con CEROS por el sector de
#  IT que genera el DataWarehouse

# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection


require("data.table")

# Parametros del script
PARAM <- list()
PARAM$dataset <- "competencia_02_tres_clases.csv.gz"
PARAM$experimento <- "kill_zero_vars"
# FIN Parametros del script

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui comienza el programa

# copio si hace falta el dataset

setwd("./")

# cargo el dataset
dataset <- fread(PARAM$dataset) # donde entreno


# creo la carpeta donde va el experimento
dir.create(paste0("./", PARAM$experimento, "/"), showWarnings = FALSE)
# Establezco el Working Directory DEL EXPERIMENTO
setwd(paste0("./", PARAM$experimento, "/"))


# ordeno el dataset
setorder(dataset, foto_mes, numero_de_cliente)

campos_buenos <- setdiff(
  colnames(dataset),
  c("numero_de_cliente", "foto_mes", "clase_ternaria","clase_binaria","clase_binaria_mixed")
)

replace_zeros_with_na <- function(x) {
#  if (all(x == 0),na.rm=TRUE) {
  if (all(is.na(x) | x == 0, na.rm = TRUE)) {
    return(NA)
  } else {
    return(x)
  }
}

# Specify the columns to be updated

# Loop through the columns and months and replace zeros with NA
for (col in campos_buenos) {
  for (month_val in unique(dataset$foto_mes)) {
    dataset[foto_mes == month_val, (col) := replace_zeros_with_na(get(col))]
  }
}

fwrite(dataset,"competencia_02_without_zerocols.csv.gz") 

quit()
