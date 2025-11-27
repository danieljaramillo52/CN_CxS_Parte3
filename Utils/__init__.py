#for cada_mes in meses_año:
#
#    df_agrup_col = TF.PandasBaseTransformer.Group_by_and_sum_cols_pd(
#        df=df_por,
#        group_col=[CONFIG_DB["cols_base"]["Mes"],"oficina_ventas","Canal","Subcanal",#"Sector","NIT","COD_CLIENTE"],
#        sum_col="Total_Gastos_CN",
#    )
#    # Encontrar el porcentaje de gasto
#    total_gasto_reg = df_agrup_col["Total_Gastos_CN"].sum()
#    
#    df_agrup_col["Porcentaje_gasto"] = (
#        df_agrup_col["Total_Gastos_CN"] / total_gasto_reg 
#    )
    
    

#print("Hola Mundo!")
#
#
#import pyarrow as pa
#import pyarrow.csv as csv
#
## Datos iniciales (ajustados para diferentes cantidades de elementos)
#regionales = ["Altiplano", "Barranquilla", "Pacifico"]  # Lista de regionales
#canales = ["Canal1", "Canal2", "Canal3", "Canal4"]  # Lista de canales
#total = 18000
#
## Suponiendo que tienes más columnas adicionales
#additional_column1 = 12345
#additional_column2 = "sample_text"
#
## Número de veces que se debe replicar cada combinación
#num_replications = 3000
#
## Crear listas separadas para cada columna
#regionals = []
#canals = []
#totals = []
#additional_col1 = []
#additional_col2 = []
#
#for regional in regionales:
#    for canal in canales:
#        regionals.extend([regional] * num_replications)
#        canals.extend([canal] * num_replications)
#        totals.extend([total] * num_replications)
#        additional_col1.extend([additional_column1] * num_replications)
#        additional_col2.extend([additional_column2] * num_replications)
#
## Crear una tabla de PyArrow
#data = {
#    "Regional": pa.array(regionals),
#    "Canal": pa.array(canals),
#    "Total": pa.array(totals),
#    "AdditionalColumn1": pa.array(additional_col1),
#    "AdditionalColumn2": pa.array(additional_col2),
#}
#
#table = pa.table(data)
#
## Guardar la tabla en un archivo CSV usando pyarrow
#csv.write_csv(table, "regional_y_canales_replicado_pyarrow.csv")
#
## Leer y mostrar la tabla para verificar (opcional)
## table_loaded = csv.read_csv("regional_y_canales_replicado_pyarrow.csv")
## print(table
#
#
## Guardar la tabla en un archivo CSV usando pyarrow
#csv.write_csv(table, "regional_y_canales_pyarrow_grande.csv")

# Leer y mostrar la tabla para verificar (opcional)
# table_loaded = csv.read_csv("regional_y_canales_pyarrow_grande.csv")
# print(table_loaded)


#import pyarrow as pa
#import pyarrow.csv as csv
#
## Datos iniciales
#regional = "Altiplano"
#canal = "Canal1"
#total = 18000
#
## Suponiendo que tienes más columnas adicionales
#additional_column1 = 12345
#additional_column2 = "sample_text"
#
## Número de veces que se debe replicar la fila
#num_replications = 3000
#
## Crear una fila de datos y replicarla
#data_row = {
#    "Regional": [regional] * num_replications,
#    "Canal": [canal] * num_replications,
#    "Total": [total] * num_replications,
#    "AdditionalColumn1": [additional_column1] * num_replications,
#    "AdditionalColumn2": [additional_column2] * num_replications,
#}
#
## Crear arrays de PyArrow a partir de los datos replicados
#data = {
#    "Regional": pa.array(data_row["Regional"]),
#    "Canal": pa.array(data_row["Canal"]),
#    "Total": pa.array(data_row["Total"]),
#    "AdditionalColumn1": pa.array(data_row["AdditionalColumn1"]),
#    "AdditionalColumn2": pa.array(data_row["AdditionalColumn2"]),
#}
#
## Crear una tabla de PyArrow
#table = pa.table(data)
#
## Guardar la tabla en un archivo CSV usando pyarrow
#csv.write_csv(table, "replicated_rows_pyarrow.csv")
#
## Leer y mostrar la tabla para verificar (opcional)
## table_loaded = csv.read_csv("replicated_rows_pyarrow.csv")
## print(table_loaded)