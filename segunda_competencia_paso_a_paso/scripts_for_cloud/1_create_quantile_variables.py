import duckdb

# Connect to DuckDB (creates an in-memory database)
con = duckdb.connect()

# Load data from CSV file into a DuckDB table
con.execute("CREATE TABLE bank_data AS SELECT * FROM read_csv_auto('competencia_02_without_zerocols.csv.gz');")
#con.execute("CREATE TABLE bank_data AS SELECT * FROM read_csv_auto('../2_undersampling_para_pruebitas_20230924/competencia_02_without_zerocols.csv');")

columns=["mrentabilidad","mrentabilidad_annual"]#,"mcomisiones","mactivos_margen","mpasivos_margen","mcuenta_corriente_adicional","mcuenta_corriente","mcaja_ahorro","mcaja_ahorro_adicional","mcaja_ahorro_dolares","mcuentas_saldo","mautoservicio","mtarjeta_visa_consumo","mtarjeta_master_consumo","mprestamos_personales","mprestamos_prendarios","mprestamos_hipotecarios","mplazo_fijo_dolares","mplazo_fijo_pesos","minversion1_pesos","minversion1_dolares","minversion2","mpayroll","mpayroll2","mcuenta_debitos_automaticos","mttarjeta_visa_debitos_automaticos","mttarjeta_master_debitos_automaticos","mpagodeservicios","mpagomiscuentas","mcajeros_propios_descuentos","mtarjeta_visa_descuentos","mtarjeta_master_descuentos","mcomisiones_mantenimiento","mcomisiones_otras","mforex_buy","mforex_sell","mtransferencias_recibidas","mtransferencias_emitidas","mextraccion_autoservicio","mcheques_depositados","mcheques_emitidos","mcheques_depositados_rechazados","mcheques_emitidos_rechazados","matm","matm_other","Master_mfinanciacion_limite","Master_msaldototal","Master_msaldopesos","Master_msaldodolares","Master_mconsumospesos","Master_mconsumosdolares","Master_mlimitecompra","Master_madelantopesos","Master_madelantodolares","Master_mpagado","Master_mpagospesos","Master_mpagosdolares","Master_mconsumototal","Master_mpagominimo","Visa_mfinanciacion_limite","Visa_msaldototal","Visa_msaldopesos","Visa_msaldodolares","Visa_mconsumospesos","Visa_mconsumosdolares","Visa_mlimitecompra","Visa_madelantopesos","Visa_madelantodolares","Visa_mpagado","Visa_mpagospesos","Visa_mpagosdolares","Visa_mconsumototal","Visa_mpagominimo"]

no_tocadas="foto_mes,numero_de_cliente, active_quarter,cliente_vip,internet,cproductos,tcuentas,ccuenta_corriente,ccaja_ahorro,cdescubierto_preacordado,ctarjeta_debito,ctarjeta_debito_transacciones,ctarjeta_visa,ctarjeta_visa_transacciones,ctarjeta_master,ctarjeta_master_transacciones,cprestamos_personales,cprestamos_prendarios,cprestamos_hipotecarios,cplazo_fijo,cinversion1,cinversion2,cpayroll2_trx,ccuenta_debitos_automaticos,ctarjeta_visa_debitos_automaticos,ctarjeta_master_debitos_automaticos,cpagodeservicios,cpagomiscuentas,ccajeros_propios_descuentos,ctarjeta_visa_descuentos,ctarjeta_master_descuentos,ccomisiones_mantenimiento,ccomisiones_otras,cforex,cforex_buy,cforex_sell,ctransferencias_recibidas,ctransferencias_emitidas,cextraccion_autoservicio,ccheques_depositados,ccheques_emitidos,ccheques_depositados_rechazados,ccheques_emitidos_rechazados,tcallcenter,ccallcenter_transacciones,thomebanking,chomebanking_transacciones,ccajas_transacciones,ccajas_consultas,ccajas_depositos,ccajas_extracciones,ccajas_otras,catm_trx,catm_trx_other,ctrx_quarter,tmobile_app,cmobile_app_trx,Master_delinquency,Master_status,Master_Fvencimiento,Master_Finiciomora,Master_fultimo_cierre,Master_fechaalta,Master_cconsumos,Master_cadelantosefectivo,Visa_delinquency,Visa_status,Visa_Fvencimiento,Visa_Finiciomora,Visa_fultimo_cierre,Visa_fechaalta,Visa_cconsumos,Visa_cadelantosefectivo"


nuevos_features=" "
for column in columns:
    nuevos_features += f"""\n, ROUND(PERCENT_RANK() OVER (PARTITION BY foto_mes ORDER BY {column})*20000,2) AS q_{column}
			"""


result=con.execute(f"""SELECT """+ no_tocadas +""" """+nuevos_features+""" 
    		FROM bank_data
""")

result_df = result.fetchdf()

# Specify the path for the new CSV file
output_csv = 'competencia_02_quantile_features.csv.gz'

# Save the DataFrame to a new CSV file
result_df.to_csv(output_csv, index=False)

print(f"Results saved to {output_csv}")
con.close()

