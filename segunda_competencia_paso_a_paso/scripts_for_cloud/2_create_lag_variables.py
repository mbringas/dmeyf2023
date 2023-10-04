import duckdb

# Connect to DuckDB (creates an in-memory database)
con = duckdb.connect()

# Load data from CSV file into a DuckDB table
con.execute("CREATE TABLE bank_data AS SELECT * FROM read_csv_auto('competencia_02_quantile_features.csv.gz');")

columns=["active_quarter","cliente_vip","internet","q_mrentabilidad","q_mrentabilidad_annual","q_mcomisiones","q_mactivos_margen","q_mpasivos_margen","cproductos","tcuentas","ccuenta_corriente","q_mcuenta_corriente_adicional","q_mcuenta_corriente","ccaja_ahorro","q_mcaja_ahorro","q_mcaja_ahorro_adicional","q_mcaja_ahorro_dolares","cdescubierto_preacordado","q_mcuentas_saldo","ctarjeta_debito","ctarjeta_debito_transacciones","q_mautoservicio","ctarjeta_visa","ctarjeta_visa_transacciones","q_mtarjeta_visa_consumo","ctarjeta_master","ctarjeta_master_transacciones","q_mtarjeta_master_consumo","cprestamos_personales","q_mprestamos_personales","cprestamos_prendarios","q_mprestamos_prendarios","cprestamos_hipotecarios","q_mprestamos_hipotecarios","cplazo_fijo","q_mplazo_fijo_dolares","q_mplazo_fijo_pesos","cinversion1","q_minversion1_pesos","q_minversion1_dolares","cinversion2","q_minversion2","cseguro_vida","cseguro_auto","cseguro_vivienda","cseguro_accidentes_personales","ccaja_seguridad","cpayroll_trx","q_mpayroll","q_mpayroll2","cpayroll2_trx","ccuenta_debitos_automaticos","q_mcuenta_debitos_automaticos","ctarjeta_visa_debitos_automaticos","q_mttarjeta_visa_debitos_automaticos","ctarjeta_master_debitos_automaticos","q_mttarjeta_master_debitos_automaticos","cpagodeservicios","q_mpagodeservicios","cpagomiscuentas","q_mpagomiscuentas","ccajeros_propios_descuentos","q_mcajeros_propios_descuentos","ctarjeta_visa_descuentos","q_mtarjeta_visa_descuentos","ctarjeta_master_descuentos","q_mtarjeta_master_descuentos","ccomisiones_mantenimiento","q_mcomisiones_mantenimiento","ccomisiones_otras","q_mcomisiones_otras","cforex","cforex_buy","q_mforex_buy","cforex_sell","q_mforex_sell","ctransferencias_recibidas","q_mtransferencias_recibidas","ctransferencias_emitidas","q_mtransferencias_emitidas","cextraccion_autoservicio","q_mextraccion_autoservicio","ccheques_depositados","q_mcheques_depositados","ccheques_emitidos","q_mcheques_emitidos","ccheques_depositados_rechazados","q_mcheques_depositados_rechazados","ccheques_emitidos_rechazados","q_mcheques_emitidos_rechazados","tcallcenter","ccallcenter_transacciones","thomebanking","chomebanking_transacciones","ccajas_transacciones","ccajas_consultas","ccajas_depositos","ccajas_extracciones","ccajas_otras","catm_trx","q_matm","catm_trx_other","q_matm_other","ctrx_quarter","tmobile_app","cmobile_app_trx","Master_delinquency","Master_status","q_Master_mfinanciacion_limite","Master_Fvencimiento","Master_Finiciomora","q_Master_msaldototal","q_Master_msaldopesos","q_Master_msaldodolares","q_Master_mconsumospesos","q_Master_mconsumosdolares","q_Master_mlimitecompra","q_Master_madelantopesos","q_Master_madelantodolares","Master_fultimo_cierre","q_Master_mpagado","q_Master_mpagospesos","q_Master_mpagosdolares","Master_fechaalta","q_Master_mconsumototal","Master_cconsumos","Master_cadelantosefectivo","q_Master_mpagominimo","Visa_delinquency","Visa_status","q_Visa_mfinanciacion_limite","Visa_Fvencimiento","Visa_Finiciomora","q_Visa_msaldototal","q_Visa_msaldopesos","q_Visa_msaldodolares","q_Visa_mconsumospesos","q_Visa_mconsumosdolares","q_Visa_mlimitecompra","q_Visa_madelantopesos","q_Visa_madelantodolares","Visa_fultimo_cierre","q_Visa_mpagado","q_Visa_mpagospesos","q_Visa_mpagosdolares","Visa_fechaalta","q_Visa_mconsumototal","Visa_cconsumos","Visa_cadelantosefectivo","q_Visa_mpagominimo"]

nuevos_features=""
for column in columns:
    nuevos_features += f"""\n, LAG({column}) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS prev_{column}
			   \n, {column} - LAG({column}) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS difference_{column}  
                           \n, regr_slope({column}, cliente_antiguedad) over ventana_6 as ctrx_{column}_slope_6
			   \n, LAG({column},3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS prev3_{column}
                           \n, LAG({column},6) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS prev6_{column}
                           \n, {column} - LAG({column},3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS difference3_{column}  
                           \n, {column} - LAG({column},6) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS difference6_{column}  
			"""


result=con.execute(f"""SELECT * """+nuevos_features+""" 
    		FROM bank_data
                window ventana_6 as (partition by numero_de_cliente order by foto_mes rows between 6 preceding and current row)
""")

result_df = result.fetchdf()

# Specify the path for the new CSV file
output_csv = 'competencia_02_historical_features.csv.gz'

# Save the DataFrame to a new CSV file
result_df.to_csv(output_csv, index=False)

print(f"Results saved to {output_csv}")
con.close()

