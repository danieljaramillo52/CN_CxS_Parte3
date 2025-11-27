# Importaciones de librerias.
import config_paths_routes
import os
import exclusive_functions as ef
import General_Functions as GF
import Transformation_Functions as TF
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
#import base_trabajar
#base_trabajar.run()

# Procesar dict_configuración del proyecto.

"""Ubicamos la ruta de ejecución."""
GF.Obtener_lugar_de_ejecucion()

config = GF.Procesar_configuracion("config.yml")


# Lectura base de Gastos.
lector_insumos = GF.ExcelReader(path=config["path_insumos"])

CONFIG_BASE_GASTOS = config["Insumos"]["Base_Gastos"]
CONFIG_DB = config["Insumos"]["db_cxs_porcentajes"]
CONFIG_DRIVER_CLI = config["Insumos"]["driver_cliente"]
DICT_CONS = config["dict_constantes"]

base_gastos = lector_insumos.Lectura_simple_excel(
    nom_insumo=CONFIG_BASE_GASTOS["nom_base"],
    nom_hoja=CONFIG_BASE_GASTOS["nom_hoja"],
)

base_gastos = TF.PandasBaseTransformer.Cambiar_tipo_dato_multiples_columnas_pd(
    base=base_gastos,
    list_columns=[CONFIG_BASE_GASTOS["cols_base"]["TOTAL_GASTOS_CN"]],
    type_data=float,
)

# Eliminar duplicados de la base Gastos.
base_gastos_agrup = TF.PandasBaseTransformer.Group_by_and_sum_cols_pd(
    df=base_gastos, group_col=CONFIG_BASE_GASTOS["cols_agrup"]["group"], sum_col = "TOTAL_GASTOS_CN")

base_gastos_sin_dup = TF.PandasBaseTransformer.Eliminar_duplicados_x_cols(
    df=base_gastos, cols=CONFIG_BASE_GASTOS["cols_agrup"]["group"]
)

driver_cliente = lector_insumos.Lectura_simple_excel(
    nom_hoja=CONFIG_DRIVER_CLI["nom_hoja"], nom_insumo=CONFIG_DRIVER_CLI["nom_base"]
)

# Cargar consulta insumo_cxs.db

# Crear instancia de la clase y ejecutar la consulta.
db = ef.ProcesarInsumoCxsDB(config=CONFIG_DB)
df_por = db.execute_query()


df_por = TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
    base=df_por, cols_to_rename=CONFIG_DB["cols_to_rename"]
)

# Duplicar gastos para Mes.
bases_meses_adicionales = []
for cada_mes, cada_reemplazo in CONFIG_DB["meses_duplicar_distribucion"].items():
    if cada_mes == cada_reemplazo:
        continue
    else:
        # Extraer el mes correspondiente
        df_por_fil_mes = TF.PandasBaseTransformer.Filtrar_por_valores_pd(
            df=df_por,
            columna=CONFIG_BASE_GASTOS["cols_base"]["MES"],
            valores_filtrar=cada_reemplazo,
        )
        # Asginar el nuevo mes.
        df_por_fil_mes.loc[:, CONFIG_BASE_GASTOS["cols_base"]["MES"]] = cada_mes

        # Agregar nueva sección a la lista de bases.
        bases_meses_adicionales.append(df_por_fil_mes)

# Concatenar base original con nuevas.
df_por_mes_compto = TF.PandasBaseTransformer.concatenar_dataframes(
    df_list=[df_por] + bases_meses_adicionales
)

# Reemplazar_valores_tipologia.
base_gastos_mod = TF.PandasBaseTransformer.Reemplazar_valores_con_dict_pd(
    df=base_gastos_sin_dup,
    columna=CONFIG_BASE_GASTOS["cols_reemplazar"]["nom_col"],
    diccionario_mapeo=CONFIG_BASE_GASTOS["cols_reemplazar"]["dict_reemp"],
)

# Reemplazar valores de meses para ajsutarlos al año actual.
base_gastos_mod = TF.PandasBaseTransformer.Reemplazar_valores_con_dict_pd(
    df=base_gastos_mod,
    columna=CONFIG_BASE_GASTOS["cols_base"]["MES"],
    diccionario_mapeo=CONFIG_BASE_GASTOS["dict_meses"],
)

base_gastos_mod_agrup = TF.PandasBaseTransformer.Group_by_and_sum_cols_pd(
    df=base_gastos_mod,
    group_col=CONFIG_BASE_GASTOS["cols_agrup"]["group"],
    sum_col=CONFIG_BASE_GASTOS["cols_agrup"]["sum"],
)

lista_dfs_claves_null = []
bases_completas = {}
for cada_caso in CONFIG_DB["dict_casos"].keys():

    base_gastos_fil = TF.PandasBaseTransformer.Filtrar_por_valores_pd(
        df=base_gastos_mod_agrup,
        columna=CONFIG_BASE_GASTOS["cols_base"]["Distribucion"],
        valores_filtrar=cada_caso,
    )
    if base_gastos_fil.empty:
        continue

    base_gastos_fil_rename = (
        TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
            base=base_gastos_fil,
            cols_to_rename=CONFIG_DB["dict_duplicados"][cada_caso]["cols_rename"],
        )
    )

    base_gastos_concat = TF.PandasBaseTransformer.concatenar_columnas_pd(
        dataframe=base_gastos_fil_rename,
        cols_elegidas=CONFIG_DB["dict_casos"][cada_caso],
        nueva_columna=DICT_CONS["Concatenada"],
    )

    df_por_concat = TF.PandasBaseTransformer.concatenar_columnas_pd(
        dataframe=df_por_mes_compto,
        cols_elegidas=CONFIG_DB["dict_casos"][cada_caso],
        nueva_columna=DICT_CONS["Concatenada"],
    )

    df_por_concat["Total_Gastos_CN"] = df_por_concat["Total_Gastos_CN"].astype(float)

    base_gastos_concat_select = TF.PandasBaseTransformer.Seleccionar_columnas_pd(
        df=base_gastos_concat,
        cols_elegidas=CONFIG_BASE_GASTOS["cols_select"],
    )

    df_dispon = TF.PandasBaseTransformer.pd_left_merge(
        base_left=base_gastos_concat_select,
        base_right=df_por_concat,
        key=DICT_CONS["Concatenada"],
    )

    total_gastos_por_llave = df_dispon.groupby(["NUMERO_CECO", "Concatenada"])[
        DICT_CONS["Total_Gastos_CN"]
    ].transform("sum")

    # Calcular y agregar Porcentje_Gastos
    df_dispon.loc[:, DICT_CONS["Porcentaje_Gastos"]] = (
        df_dispon[DICT_CONS["Total_Gastos_CN"]] / total_gastos_por_llave
    )

    # Calcular y agregar Distribucion_Gastos
    df_dispon.loc[:, DICT_CONS["Distribucion_Gastos"]] = (
        df_dispon[DICT_CONS["Porcentaje_Gastos"]] * df_dispon["TOTAL_GASTOS_CN"]
    )

    # Determinar claves que no cruzaron.
    filas_faltantes = df_dispon[df_dispon[DICT_CONS["Distribucion_Gastos"]].isnull()]

    df_claves_nulas = TF.PandasBaseTransformer.Seleccionar_columnas_pd(
        df=filas_faltantes,
        cols_elegidas=[DICT_CONS["Concatenada"], DICT_CONS["Distribucion"]],
    )

    df_claves_nulas_unicas = df_claves_nulas.drop_duplicates()

    df_claves_nulas_unicas_copy = df_claves_nulas_unicas.copy()

    if not df_claves_nulas_unicas_copy.empty:
        df_claves_nulas_unicas_copy.loc[:, f'{DICT_CONS["Concatenada"]}_caso'] = (
            " ".join(CONFIG_DB["dict_casos"][cada_caso])
        )

    # Eliminar nulos no cruzados
    df_dispon_fil = TF.PandasBaseTransformer.remove_duplicates(df=df_dispon)

    # Traer columna de código de cliente.

    # Eliminar columnas innecesarias.
    df_dispon_select = TF.PandasBaseTransformer.Eliminar_columnas_pd(
        df=df_dispon,
        columnas_a_eliminar=[
            DICT_CONS["Total_Gastos_CN"],
            DICT_CONS["Porcentaje_Gastos"],
        ],
    )
    df_dispon_sin_null = df_dispon_select.dropna(
        subset=DICT_CONS["Distribucion_Gastos"]
    )

    # Agregar a diccionarios
    bases_completas[cada_caso] = df_dispon_sin_null
    lista_dfs_claves_null.append(df_claves_nulas_unicas_copy)

# Concatenar las bases de todos los casos.
base_distribuida = TF.PandasBaseTransformer.concatenar_dataframes(
    df_list=bases_completas.values()
)

del bases_completas

# Reemplazar valores columna Cliente.
dict_driver = GF.Crear_diccionario_desde_dataframe(
    df=driver_cliente,
    col_clave=CONFIG_DRIVER_CLI["cols_necesarias"]["Cliente"],
    col_valor=CONFIG_DRIVER_CLI["cols_necesarias"]["Nom_cliente"],
)

base_distribuida = TF.PandasBaseTransformer.Reemplazar_valores_con_dict_pd(
    df=base_distribuida, columna=DICT_CONS["Cliente"], diccionario_mapeo=dict_driver
)
driver_cliente.loc[:, CONFIG_DRIVER_CLI["cols_necesarias"]["Cliente"]] = driver_cliente[
    CONFIG_DRIVER_CLI["cols_necesarias"]["Nom_cliente"]
]

# Traer columna codigo de cliente.
base_distribuida_final = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_distribuida,
    base_right=driver_cliente[[DICT_CONS["Cliente"], DICT_CONS["Cod_Cliente"]]],
    key=DICT_CONS["Cliente"],
)

# Completar nulos columna Cod_Cliente.
base_distribuida_final[DICT_CONS["Cod_Cliente"]] = base_distribuida_final[
    DICT_CONS["Cod_Cliente"]
].fillna(DICT_CONS["Sin asignar"])

del driver_cliente

# Eliminar columna TOTAL_GASTOS_CN ya innecesaria, la columna que tomará este nombre es "Distribución Gastos"

base_distribuida_final = TF.PandasBaseTransformer.Eliminar_columnas_pd(
    df=base_distribuida_final,
    columnas_a_eliminar=CONFIG_BASE_GASTOS["cols_base"]["TOTAL_GASTOS_CN"],
)

base_distribuida_final_rename = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=base_distribuida_final,
        cols_to_rename=CONFIG_BASE_GASTOS["cols_rename_finales"],
    )

)
base_distribuida_final_rename.loc[:,"TIPO"] = "Presupuesto"
base_distribuida_final_rename.loc[:,"AGRUPACION"] = "-"
base_distribuida_final_reorder = TF.PandasBaseTransformer.Seleccionar_columnas_pd(
        df=base_distribuida_final_rename,
        cols_elegidas=CONFIG_BASE_GASTOS["orden_final_cols"],
    )

# Concatenar df claves no cruzadas.
df_claves_no_cruzadas = TF.PandasBaseTransformer.concatenar_dataframes(
    df_list=lista_dfs_claves_null
)
del lista_dfs_claves_null

# Exportar a excel el archivo con las claves faltantes.
df_claves_no_cruzadas.to_excel("Resultados/claves_faltantes.xlsx", index=False)

db_manager = GF.DatabaseManager()

db_manager.insert_dataframe(
    dataframe=base_distribuida_final_reorder, table_name="td_cxs_dinamico_ventas", schema="cxs"
)


