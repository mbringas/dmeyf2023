# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("rlist")

# defino los parametros de la corrida, en una lista, la variable global  PARAM
#  muy pronto esto se leera desde un archivo formato .yaml
PARAM <- list()

PARAM$experimento <- "HT5230"

PARAM$input$dataset <- "../../segunda_competencia/competencia_02_tres_clases.csv"
set.seed(123)  # Set a seed for reproducibility
# un undersampling de 0.1  toma solo el 10% de los CONTINUA
PARAM$trainingstrategy$undersampling <- 0.1
PARAM$trainingstrategy$semilla_azar <- 102191 # Aqui poner su  primer  semilla

#------------------------------------------------------------------------------

# cargo el dataset donde voy a entrenar el modelo
dataset <- fread(PARAM$input$dataset)

# Step 1: Filter clients who never abandon
clients_continua <- dataset[, .(foto_mes = .N,clase_ternaria), by = numero_de_cliente][all(clase_ternaria == "CONTINUA"), numero_de_cliente]
clients_baja <- dataset[.(c("BAJA+1", "BAJA+2")), on = .(clase_ternaria), unique(numero_de_cliente)]

# Step 2: Randomly select 10% of these clients
sample_clients <- sample(clients_continua, round(0.10 * length(clients_continua)))

# Step 3: Combine the two sets of clients
selected_clients <- union(sample_clients, clients_baja)

# Step 4: Filter the original dataset for the selected clients
filtered_data <- dataset[numero_de_cliente %in% selected_clients]

fwrite(filtered_data,"undersampled_10pct.csv")
quit()

