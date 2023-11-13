import duckdb

# Connect to DuckDB (creates an in-memory database)
con = duckdb.connect()

# Load data from CSV file into a DuckDB table
con.execute("CREATE TABLE bank_data AS SELECT * FROM read_csv_auto('~/buckets/b1/datasets/colaborativos_withTarget.csv.gz');")

intact=["clase_ternaria","foto_mes","numero_de_cliente"]

columns=["active_quarter","cliente_vip","internet","cproductos","tcuentas","ccuenta_corriente","ccaja_ahorro","mcaja_ahorro_adicional",
	"mcaja_ahorro_dolares","cdescubierto_preacordado","ctarjeta_debito","ctarjeta_debito_transacciones","ctarjeta_visa",
	"ctarjeta_visa_transacciones","ctarjeta_master","ctarjeta_master_transacciones","cprestamos_personales",
	"cprestamos_prendarios","cprestamos_hipotecarios","cplazo_fijo","cinversion1","cinversion2","minversion2",
	"cseguro_vida","cseguro_auto","cseguro_vivienda","cseguro_accidentes_personales","ccaja_seguridad","cpayroll_trx",
	"mpayroll2","cpayroll2_trx","ccuenta_debitos_automaticos","ctarjeta_visa_debitos_automaticos","ctarjeta_master_debitos_automaticos",
	"cpagodeservicios","cpagomiscuentas","mpagomiscuentas","ccajeros_propios_descuentos","mcajeros_propios_descuentos","ctarjeta_visa_descuentos",
	"mtarjeta_visa_descuentos","ctarjeta_master_descuentos","mtarjeta_master_descuentos","ccomisiones_mantenimiento",
	"ccomisiones_otras","cforex","cforex_buy","mforex_buy","cforex_sell","mforex_sell","ctransferencias_recibidas",
	"ctransferencias_emitidas","cextraccion_autoservicio","ccheques_depositados","ccheques_emitidos","ccheques_depositados_rechazados",
	"ccheques_emitidos_rechazados","tcallcenter","ccallcenter_transacciones","thomebanking","chomebanking_transacciones","ccajas_transacciones",
	"ccajas_consultas","ccajas_depositos","ccajas_extracciones","ccajas_otras","catm_trx","catm_trx_other","matm_other","ctrx_quarter",
	"tmobile_app","cmobile_app_trx","Master_delinquency","Master_status","Master_mfinanciacion_limite","Master_Fvencimiento","Master_Finiciomora",
	"Master_msaldopesos","Master_msaldodolares","Master_mconsumospesos","Master_mconsumosdolares","Master_mlimitecompra",
	"Master_madelantopesos","Master_madelantodolares","Master_fultimo_cierre","Master_mpagospesos","Master_mpagosdolares",
	"Master_fechaalta","Master_mconsumototal","Master_cconsumos","Master_cadelantosefectivo","Master_mpagominimo","Visa_delinquency",
	"Visa_status","Visa_mfinanciacion_limite","Visa_Fvencimiento","Visa_Finiciomora","Visa_msaldopesos","Visa_msaldodolares",
	"Visa_mconsumospesos","Visa_mconsumosdolares","Visa_mlimitecompra","Visa_madelantopesos","Visa_madelantodolares","Visa_fultimo_cierre",
	"Visa_mpagospesos","Visa_mpagosdolares","Visa_fechaalta","Visa_mconsumototal","Visa_cconsumos","Visa_cadelantosefectivo",
	"Visa_mpagominimo","mcuenta_corriente_adicional","mcuenta_corriente","mcaja_ahorro","mcuentas_saldo","mprestamos_personales","mprestamos_prendarios",
	 "mprestamos_hipotecarios","mplazo_fijo_dolares","mplazo_fijo_pesos","minversion1_pesos","minversion1_dolares",
	 "mttarjeta_visa_debitos_automaticos","mttarjeta_master_debitos_automaticos","mpagodeservicios","mtransferencias_recibidas",
	 "mtransferencias_emitidas","mextraccion_autoservicio","mcheques_depositados","mcheques_emitidos","mcheques_depositados_rechazados",
	 "mcheques_emitidos_rechazados","matm","Visa_mpagado","Master_mpagado","Master_msaldototal","Visa_msaldototal"]
	
columns=columns+["mrentabilidad","mrentabilidad_annual","mcomisiones","mactivos_margen","mpasivos_margen","mautoservicio",
		 "mtarjeta_visa_consumo","mtarjeta_master_consumo","mpayroll","mcuenta_debitos_automaticos",
		 "mcomisiones_mantenimiento","mcomisiones_otras"]


nuevos_features=",".join(intact)
for column in columns:
    nuevos_features += f"""\n, {column} - LAG({column}) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS difference_{column}
                           \n, {column} - LAG({column},3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS difference3_{column}  
                           \n, {column} - LAG({column},6) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS difference6_{column}  
			"""


result=con.execute(f"""SELECT * """+nuevos_features+""" 
    		FROM bank_data
                window ventana_6 as (partition by numero_de_cliente order by foto_mes rows between 6 preceding and current row)
""")
print("ACA FETCHEA")
result_df = result.fetchdf()
print("ARRANCA A ESCRIBIR")
# Specify the path for the new CSV file
output_csv = '~/buckets/b1/datasets/colaborativos_features.csv.gz'

# Save the DataFrame to a new CSV file
result_df.to_csv(output_csv, index=False)

print(f"Results saved to {output_csv}")
con.close()
quit()
