# Trasformaciones realizadas a la base de precios.

import calendar
import pandas as pd
import numpy as np
import pyarrow as pa
import operator
from loguru import logger
from typing import List, Any
from datetime import datetime
from functools import reduce
from General_Functions import Registro_tiempo, ErrorHandler


class PandasBaseTransformer:

    @staticmethod
    def remove_duplicates(df):
        """
        Elimina filas duplicadas de un DataFrame y restablece el índice.

        Args:
            df (pandas.DataFrame): El DataFrame del cual eliminar duplicados y resetear el índice.

        Returns:
            pandas.DataFrame: Un nuevo DataFrame sin duplicados y con el índice reseteado.
        """
        # Eliminar duplicados
        df = df.drop_duplicates()

        # Restablecer el índice
        df = df.reset_index(drop=True)

        return df
    
    @staticmethod
    def Eliminar_duplicados_x_cols(df: pd.DataFrame, cols: list):
        """
        Elimina filas duplicadas de un DataFrame y restablece el índice.

        Args:
            df (pandas.DataFrame): El DataFrame del cual eliminar duplicados y resetear el índice.

        Returns:
            pandas.DataFrame: Un nuevo DataFrame sin duplicados y con el índice reseteado.
        """
        # Eliminar duplicados
        df = df.drop_duplicates(subset=cols)

        # Restablecer el índice
        df = df.reset_index(drop=True)

        return df


    @staticmethod
    def Cambiar_tipo_dato_multiples_columnas_pd(
        base: pd.DataFrame, list_columns: list, type_data: type
    ) -> pd.DataFrame:
        """
        Función que toma un DataFrame, una lista de sus columnas para hacer un cambio en el tipo de dato de las mismas.

        Args:
            base (pd.DataFrame): DataFrame que es la base del cambio.
            list_columns (list): Columnas a modificar su tipo de dato.
            type_data (type): Tipo de dato al que se cambiarán las columnas (ejemplo: str, int, float).

        Returns:
            pd.DataFrame: Copia del DataFrame con los cambios.
        """
        try:
            # Verificar que el DataFrame tenga las columnas especificadas
            for columna in list_columns:
                if columna not in base.columns:
                    raise KeyError(f"La columna '{columna}' no existe en el DataFrame.")

            # Cambiar el tipo de dato de las columnas
            base_copy = (
                base.copy()
            )  # Crear una copia para evitar problemas de SettingWithCopyWarning
            base_copy[list_columns] = base_copy[list_columns].astype(type_data)

            return base_copy

        except Exception as e:
            logger.critical(f"Error en Cambiar_tipo_dato_multiples_columnas: {e}")

    @staticmethod
    def cols_a_num_seleccionado(df):
        """
        Intenta convertir todas las columnas de un DataFrame a tipo numérico.
        Si una columna no puede ser convertida completamente a numérico, se deja sin cambios.

        Args:
        df (pd.DataFrame): DataFrame que contiene las columnas a convertir.

        Returns:
        pd.DataFrame: DataFrame con las columnas convertidas a tipo numérico cuando es posible.
        """
        for col in df.columns:
            try:
                if not isinstance(df, pd.DataFrame):
                    raise TypeError(
                        "El argumento 'df' debe ser un DataFrame de pandas."
                    )

                df[col] = df[col].astype(float)
            except ValueError:
                pass  # Si no puede convertir la columna, pasa a la siguiente
        return df

    @staticmethod
    def Renombrar_columnas_con_diccionario(
        base: pd.DataFrame, cols_to_rename: dict
    ) -> pd.DataFrame:
        """Funcion que toma un diccionario con keys ( nombres actuales ) y values (nuevos nombres) para remplazar nombres de columnas en un dataframe.
        Args:
            base: dataframe al cual se le harán los remplazos
            cols_to_rename: diccionario con nombres antiguos y nuevos
        Result:
            base_renombrada: Base con las columnas renombradas.
        """
        base_renombrada = None

        try:
            base_renombrada = base.rename(columns=cols_to_rename, inplace=False)
            #logger.success("Proceso de renombrar columnas satisfactorio: ")
        except Exception:
            #logger.critical("Proceso de renombrar columnas fallido.")
            raise Exception

        return base_renombrada

    def concatenate_dataframes(dataframes: list) -> pd.DataFrame:
        """
        Concatena dos DataFrames de pandas.

        Args:
            df1 (pd.DataFrame): Primer DataFrame.
            df2 (pd.DataFrame): Segundo DataFrame.

        Returns:
            pd.DataFrame: DataFrame resultante después de la concatenación.
        """
        try:
            # Concatenar por filas (verticalmente)
            df_concatenado = pd.concat(dataframes, ignore_index=True)

            # Registrar un mensaje informativo
            #logger.info("DataFrames concatenados exitosamente.")

            return df_concatenado
        except Exception as e:
            # Registrar un mensaje de error
            #logger.error(f"Error al concatenar DataFrames: {str(e)}")
            return None

    @staticmethod
    @Registro_tiempo
    def Group_by_and_sum_cols_pd(df=pd.DataFrame, group_col=list, sum_col=list):
        """
        Agrupa un DataFrame por una columna y calcula la suma de otra columna.

        Args:
            df (pandas.DataFrame): El DataFrame que se va a agrupar y sumar.
            group_col (list or str): El nombre de la columna o lista de nombres de columnas por la cual se va a agrupar.
            sum_col (list or str): El nombre de la columna o lista de nombres de columnas que se va a sumar.

        Returns:
            pandas.DataFrame: El DataFrame con las filas agrupadas y la suma calculada.
        """

        try:
            if isinstance(group_col, str):
                group_col = [group_col]

            if isinstance(sum_col, str):
                sum_col = [sum_col]

            result_df = df.groupby(group_col, as_index=False)[sum_col].sum()

            # Registro de éxito
            logger.info(f"Agrupación y suma realizadas con éxito en las columnas.")

        except Exception as e:
            # Registro de error crítico
            logger.critical(
                f"Error al realizar la agrupación y suma en las columnas. {e}"
            )
            result_df = None

        return result_df

    @Registro_tiempo
    def Transform_dfs_pandas_a_pyarrow(df: pd.DataFrame) -> pa.Table:
        """
        Función que toma un DataFrame de pandas y lo transforma en una tabla de PyArrow,
        asegurándose de que no se transfiera ningún MultiIndex como una columna.

        Args:
        df (pd.DataFrame): DataFrame de pandas a ser transformado.

        Returns:
        pa.Table: Tabla bidimensional de datos de PyArrow.
        """
        try:
            # Verificar si la entrada es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

            # Resetear el índice si es MultiIndex para evitar columnas adicionales en PyArrow
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()

            # Convertir el DataFrame de pandas a una tabla de PyArrow
            table = pa.Table.from_pandas(df, preserve_index=False)
        except Exception as e:
            raise ValueError(f"Error al convertir el DataFrame a PyArrow: {e}") from e

        return table

    @staticmethod
    def Seleccionar_columnas_pd(
        df: pd.DataFrame, cols_elegidas: List[str]
    ) -> pd.DataFrame | None:
        """
        Filtra y retorna las columnas especificadas del DataFrame.

        Parámetros:
        dataframe (pd.DataFrame): DataFrame del cual se filtrarán las columnas.
        cols_elegidas (list): Lista de nombres de las columnas a incluir en el DataFrame filtrado.

        Retorna:
        pd.DataFrame: DataFrame con las columnas filtradas.
        """
        try:
            # Verificar si dataframe es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    "El argumento 'dataframe' debe ser un DataFrame de pandas."
                )

            # Filtrar las columnas especificadas
            df_filtrado = df[cols_elegidas]

            # Registrar el proceso
            logger.info(f"Columnas filtradas: {', '.join(cols_elegidas)}")

            return df_filtrado

        except KeyError as ke:
            error_message = f"Error: Columnas especificadas no encontradas en el DataFrame: {str(ke)}"
            ErrorHandler.log_error(e, error_message)
            return df
        except Exception as e:
            logger.critical(f"Error inesperado al filtrar columnas: {str(e)}")

    @Registro_tiempo
    def Crear_diccionario_desde_dataframe(
        df: pd.DataFrame, col_clave: str, col_valor: str
    ) -> dict:
        """
        Crea un diccionario a partir de un DataFrame utilizando dos columnas especificadas.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.
            col_clave (str): El nombre de la columna que se utilizará como clave en el diccionario.
            col_valor (str): El nombre de la columna que se utilizará como valor en el diccionario.

        Returns:
            dict: Un diccionario creado a partir de las columnas especificadas.
        """
        try:
            # Verificar si las columnas existen en el DataFrame
            if col_clave not in df.columns or col_valor not in df.columns:
                raise ValueError(
                    "Las columnas especificadas no existen en el DataFrame."
                )

            # Crear el diccionario a partir de las columnas especificadas
            resultado_dict = df.set_index(col_clave)[col_valor].to_dict()

            return resultado_dict

        except ValueError as ve:
            # Registrar un mensaje crítico si hay un error
            logger.critical(f"Error: {ve}")
            raise ve

    def convertir_df_col_a_fecha(df: pd.DataFrame, columna: str):
        """
        Convierte una columna de un DataFrame de cadenas de texto a fechas.

        Args:
        df (pd.DataFrame): DataFrame que contiene la columna a convertir.
        columna (str): Nombre de la columna que contiene las fechas en formato de cadena.

        Returns:
        pd.DataFrame: DataFrame con la columna convertida a formato de fecha.

        Raises:
        ValueError: Si la conversión de fecha falla.
        KeyError: Si la columna especificada no existe en el DataFrame.
        """
        try:
            # Verificar si la columna existe en el DataFrame
            if columna not in df.columns:
                logger.error(f"La columna '{columna}' no existe en el DataFrame")
                raise KeyError(f"La columna '{columna}' no existe en el DataFrame")

            # Convertir a formato de fecha
            df[columna] = pd.to_datetime(
                df[columna], format="%d.%m.%Y", errors="coerce"
            )
            logger.info(f"Columna '{columna}' convertida a formato de fecha")

        except Exception as e:
            logger.critical(f"Error al convertir la columna '{columna}' a fecha: {e}")
            raise ValueError(
                f"Error al convertir la columna '{columna}' a fecha: {ValueError}"
            )

        return df

    def mask_fechas_mes_elegido(df: pd.DataFrame, columna: str, mes_a_medir: int):
        """
        Obtiene una máscara que indica las filas donde las fechas en una columna están vigentes en el mes actual

        Args:
        df (pd.DataFrame): DataFrame que contiene la columna a filtrar.
        columna (str): Nombre de la columna que contiene las fechas.

        Returns:
        pd.Series: Máscara booleana indicando las filas que cumplen la condición.

        Raises:
        ValueError: Si la columna especificada no es de tipo datetime.
        KeyError: Si la columna especificada no existe en el DataFrame.
        """
        try:
            # Verificar si la columna existe en el DataFrame
            if columna not in df.columns:
                logger.error(f"La columna '{columna}' no existe en el DataFrame")
                raise KeyError(f"La columna '{columna}' no existe en el DataFrame")

            # Verificar si la columna es de tipo datetime
            if not pd.api.types.is_datetime64_any_dtype(df[columna]):
                logger.error(f"La columna '{columna}' no es de tipo datetime")
                raise ValueError(f"La columna '{columna}' no es de tipo datetime")

            # Obtener el primer día del mes actual
            primer_dia_mes_actual = datetime(datetime.now().year, mes_a_medir, 1)

            """ _, ignora el primer elemento devuelto por calendar.monthrange
            solo interesa el segundo elemento de la tupla."""
            _, ultimo_dia = calendar.monthrange(
                primer_dia_mes_actual.year, primer_dia_mes_actual.month
            )

            ultimo_dia_mes_actual = datetime(
                datetime.now().year, mes_a_medir, ultimo_dia
            )

            # Crear la máscara basada en la condición

            mascara = (df[columna] >= primer_dia_mes_actual) & (
                df[columna] <= ultimo_dia_mes_actual
            )

        except Exception as e:
            logger.error(f"Error al crear la máscara para la columna '{columna}': {e}")
            raise ValueError(
                f"Error al crear la máscara para la columna '{columna}': {e}"
            )

        return mascara

    @staticmethod
    def Filtrar_por_valores_pd(
        df: pd.DataFrame, columna: str, valores_filtrar: List[str | int]
    ) -> pd.DataFrame:
        """
        Filtra el DataFrame basándose en los valores de una columna específica.

        Args:
            columna (pd.Series): Columna del DataFrame a filtrar.
            valores_filtrar (List[Union[str, int]]): Lista de valores a utilizar para filtrar la columna.

        Returns:
            pd.DataFrame: DataFrame filtrado basándose en los valores especificados.
        """
        try:
            if isinstance(valores_filtrar, str):
                valores_filtrar = [valores_filtrar]

            # Filtrar el DataFrame basándose en los valores de la columna
            df_filtrado = df[df[columna].isin(valores_filtrar)]

            return df_filtrado

        except Exception as e:
            logger.critical(f"Error inesperado al filtrar por valores: {str(e)}")
            return None

    def Filtrar_por_valores_excluidos(
        df: pd.DataFrame, columna: str, valores_excluir: List[str | int]
    ) -> pd.DataFrame:
        """
        Filtra el DataFrame excluyendo las filas que contienen valores especificados en una columna.

        Args:
            columna (pd.Series): Columna del DataFrame a filtrar.
            valores_excluir (List[Union[str, int]]): Lista de valores a excluir en el filtro.

        Returns:
            pd.DataFrame: DataFrame filtrado excluyendo las filas con valores especificados.
        """
        try:
            if isinstance(valores_excluir, str):
                valores_excluir = [valores_excluir]
            # Filtrar el DataFrame excluyendo las filas con valores especificados
            df_filtrado = df[~df[columna].isin(valores_excluir)]

            return df_filtrado

        except Exception as e:
            logger.critical(
                f"Error inesperado al filtrar por valores excluidos: {str(e)}"
            )
            return None

    def Obtener_unicos_serie_pd(df: pd.DataFrame, nombre_columna: str) -> list:
        """
        Obtiene una lista de valores únicos de una columna específica de un DataFrame.

        Args:
            df (pd.DataFrame): El DataFrame del cual se obtendrán los valores únicos.
            nombre_columna (str): El nombre de la columna de la cual se obtendrán los valores únicos.

        Returns:
            list: Una lista de valores únicos presentes en la columna especificada del DataFrame.

        Raises:
            KeyError: Si la columna especificada no existe en el DataFrame.
            TypeError: Si los tipos de datos proporcionados no son los esperados.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
            if not isinstance(nombre_columna, str):
                raise TypeError(
                    "El argumento 'nombre_columna' debe ser una cadena de caracteres."
                )
            if nombre_columna not in df.columns:
                raise KeyError(
                    f"La columna '{nombre_columna}' no se encuentra en el DataFrame."
                )

            return df[nombre_columna].unique().tolist()
        except KeyError as ke:
            print(f"Error: {ke}")
            raise
        except TypeError as te:
            print(f"Error de tipo de datos: {te}")
            raise

    def combinar_mascaras(mascaras, operador="and"):
        """
        Combina una lista de máscaras booleanas utilizando el operador especificado.

        Args:
        mascaras (list of pd.Series): Lista de máscaras booleanas.
        operador (str): Operador lógico para combinar las máscaras ('and', 'or').

        Returns:
        pd.Series: Máscara booleana combinada.

        Raises:
        ValueError: Si el operador especificado no es válido.
        """
        if operador == "and":
            operador_func = operator.and_
        elif operador == "or":
            operador_func = operator.or_
        else:
            raise ValueError(f"Operador '{operador}' no válido. Use 'and' o 'or'.")

        return reduce(operador_func, mascaras)

    def Filtrar_por_operacion(
        df: pd.DataFrame, columna: str, valor_umbral: float, operacion: str
    ) -> pd.DataFrame:
        """
        Filtra un DataFrame manteniendo solo las filas donde los valores en una columna específica cumplen con una operación de comparación.
        """
        # Verifica que la columna exista
        if columna not in df.columns:
            raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

        # Mapea las operaciones a funciones de comparación
        operaciones = {
            "mayor": operator.gt,  # Greater than ("Mayor que")
            "menor": operator.lt,  # Less than ("Menor que")
            "mayor_igual": operator.ge,  # Greater than or equal to
            "menor_igual": operator.le,  # Less than or equal to
        }

        # Selecciona la función de comparación
        if operacion not in operaciones:
            raise ValueError(
                f"Operación inválida: '{operacion}', Solo puedes ingresar las operaciones de esta lista: ['mayor', 'menor' 'mayor_igual', 'menor_igual'] "
            )
        comparar = operaciones[operacion]

        # Crea la máscara utilizando la función de comparación
        mask = comparar(df[columna], valor_umbral)

        # Filtra el DataFrame
        df_filtrado = df[mask]

        return df_filtrado

    @Registro_tiempo
    def Filtrar_df_dict_clave_valor(df, filtros):
        """
        Filtra el DataFrame basado en un diccionario de condiciones.
        Cada condición puede incluir múltiples valores posibles para cada columna.

        Args:
        df (pd.DataFrame): DataFrame a filtrar.
        filtros (dict): Diccionario con las columnas y los valores (lista) a filtrar.

        Returns:
        pd.DataFrame: DataFrame filtrado.
        """
        mask = pd.Series([True] * len(df))
        for columna, valores in filtros.items():
            if isinstance(valores, list):
                mask &= df[columna].isin(valores)
            else:
                mask &= df[columna] == valores
        return df[mask]

    @staticmethod
    def agregar_cadena_a_serie_pd(df: pd.DataFrame, nom_col: str, cadena: str):
        """
        Agrega una cadena a cada elemento de una serie en un DataFrame de pandas.

        Parámetros:
        df (pd.DataFrame): El DataFrame al que se le va a agregar la cadena.
        nom_col (str): El nombre de la serie a la que se le va a agregar la cadena.
        cadena (str): La cadena que se va a agregar a cada elemento de la serie.

        Retorna:
        pd.DataFrame: El DataFrame actualizado.
        """
        if nom_col not in df.columns:
            logger.error(f"La columna {nom_col} no esta en el dataframe")
            raise ValueError(f"La columna {nom_col} no existe en el DataFrame.")

        df[nom_col] = df[nom_col] + cadena
        logger.success(f"Agregada la cadena {cadena} a toda la serie {nom_col}")
        return df

    @staticmethod
    def pd_left_merge(
        base_left: pd.DataFrame, base_right: pd.DataFrame, key: str
    ) -> pd.DataFrame:
        """Función que retorna el left join de dos dataframe de pandas.

        Args:
            base_left (pd.DataFrame): Dataframe que será la base del join.
            base_right (pd.DataFrame): Dataframe del cuál se extraerá la información    complementaria.
            key (str): Llave mediante la cual se va a realizar el merge o join.

        Returns:
            pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
        """

        # Validar que base_left y base_right sean DataFrames de pandas
        if not isinstance(base_left, (pd.DataFrame, pd.Series)):
            raise ValueError("El argumento base_left no es un DataFrame de pandas")
        if not isinstance(base_right, (pd.DataFrame, pd.Series)):
            raise ValueError("El argumento base_right no es un DataFrame de pandas")

        base = None

        try:
            base = pd.merge(left=base_left, right=base_right, how="left", on=key)
            logger.success("Proceso de merge satisfactorio")
        except pd.errors.MergeError as e:
            logger.critical(f"Proceso de merge fallido: {e}")
            raise e

        return base

    def pd_left_merge_two_keys(
        base_left: pd.DataFrame,
        base_right: pd.DataFrame,
        left_key: str,
        right_key: str,
    ) -> pd.DataFrame:
        """Función que retorna el left join de dos dataframe de pandas.

        Args:
            base_left (pd.DataFrame): Dataframe que será la base del join.
            base_right (pd.DataFrame): Dataframe del cuál se extraerá la información complementaria.
            key (str): Llave mediante la cual se va a realizar el merge o join.

        Returns:
            pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
        """

        # Validar que base_left y base_right sean DataFrames de pandas
        if not isinstance(base_left, (pd.DataFrame, pd.Series)):
            raise ValueError("El argumento base_left no es un DataFrame de pandas")
        if not isinstance(base_right, (pd.DataFrame, pd.Series)):
            raise ValueError("El argumento base_right no es un DataFrame de pandas")

        base = None

        try:
            base = pd.merge(
                left=base_left,
                right=base_right,
                how="left",
                left_on=left_key,
                right_on=right_key,
            )
            logger.success("Proceso de merge satisfactorio")
        except pd.errors.MergeError as e:
            logger.critical(f"Proceso de merge fallido: {e}")
            raise e

        return base

    @staticmethod
    def merge_dfs_on_column(df_list: List[pd.DataFrame], key: str):
        """
        Fusiona una lista de DataFrames en uno solo, utilizando una columna específica
        como clave para el merge. Si la lista está vacía, devuelve None.

        Parámetros:
        - df_list (list of pd.DataFrame): Lista de DataFrames para fusionar.
        - key (str): Nombre de la columna en la que se basará el merge.

        Retorna:
        - pd.DataFrame: DataFrame resultante de la fusión de todos los DataFrames de la lista.
        """

        # Realiza la fusión (merge) sucesiva de todos los DataFrames en la lista
        df_merged = reduce(
            lambda left, right: pd.merge(left, right, on=key, how="left"),
            df_list,
        )

        return df_merged

    def Eliminar_filas_con_cadena(df: pd.DataFrame, columna: str, cadena: str):
        """
        Elimina todas las filas que contengan una palabra específica en una columna del DataFrame.

        Args:
            - df_name (str): Nombre del DataFrame.
            - columna (str): Nombre de la columna en la que se realizará la búsqueda.
            - cadena (str): Palabra específica que se utilizará como criterio de eliminación.

        Returns:
            pd.DataFrame: Nuevo DataFrame sin las filas que contienen la palabra especificada.
        """
        try:
            # Eliminar filas que contengan la palabra en la columna especificada
            df_filtrado = df[
                ~df[columna].str.contains(rf"\b{cadena}\b", case=False, regex=True)
            ]

            # Registrar información sobre las filas eliminadas
            logger.info(
                f"Filas que contienen '{cadena}' en la columna '{columna}' eliminadas con éxito."
            )

            return df_filtrado

        except KeyError as ke:
            # Registrar un error específico si la columna no existe
            logger.critical(f"Error al eliminar filas: {str(ke)}")
            # Propagar la excepción para que el usuario sea consciente del problema
            raise ke

    def concatenar_columnas_pd(
        dataframe: pd.DataFrame, cols_elegidas: List[str], nueva_columna: str
    ) -> pd.DataFrame | None:
        """
        Concatena las columnas especificadas y agrega el resultado como una nueva columna al DataFrame.

        Parámetros:
        dataframe (pd.DataFrame): DataFrame del cual se concatenarán las columnas.
        cols_elegidas (list): Lista de nombres de las columnas a concatenar.
        nueva_columna (str): Nombre de la nueva columna que contendrá el resultado de la concatenación.

        Retorna:
        pd.DataFrame: DataFrame con la nueva columna agregada.
        """
        try:
            # Verificar si dataframe es un DataFrame de pandas
            if not isinstance(dataframe, pd.DataFrame):
                raise TypeError(
                    "El argumento 'dataframe' debe ser un DataFrame de pandas."
                )

            # Verificar si las columnas especificadas existen en el DataFrame
            for col in cols_elegidas:
                if col not in dataframe.columns:
                    raise KeyError(f"La columna '{col}' no existe en el DataFrame.")

            # Concatenar las columnas especificadas y agregar el resultado como una nueva columna
            dataframe[nueva_columna] = (
                dataframe[cols_elegidas].fillna("").agg("".join, axis=1)
            )

            # Registrar el proceso
            #logger.info(
            #    f"Columnas '{', '.join(cols_elegidas)}' concatenadas y almacenadas en '{nueva_columna}'."
            #)

            return dataframe

        except Exception as e:
            #logger.critical(f"Error inesperado al concatenar columnas: {str(e)}")
            return None

    def Remplazar_nulos_multiples_columnas_pd(
        base: pd.DataFrame, list_columns: list, value: str
    ) -> pd.DataFrame:
        base_modificada = None
        """Funcion que toma un dataframe, una lista de sus columnas para hacer un 
        cambio en los datos nulos de las mismas.
        Args:
            base: Dataframe a base del cambio.
            list_columns: Columnas a modificar su tipo de dato.
            Value: valor del dato: (Notar, solo del tipo str.) 
        Returns: 
            base_modificada (copia de la base con los cambios.)
        """
        try:
            base.loc[:, list_columns] = base[list_columns].fillna(value)
            base_modificada = base
            logger.success("cambio tipo de dato satisfactorio: ")

        except Exception:
            logger.critical("cambio tipo de dato fallido.")
            raise Exception

        return base_modificada

    def Renombrar_columnas_con_diccionario(
        base: pd.DataFrame, cols_to_rename: dict
    ) -> pd.DataFrame:
        """
        Función que toma un diccionario con keys (nombres actuales) y values (nuevos nombres) para reemplazar nombres de columnas en un DataFrame.

        Args:
            base: DataFrame al cual se le harán los reemplazos.
            cols_to_rename: Diccionario con nombres antiguos y nuevos.

        Result:
            base_renombrada: Base con las columnas renombradas.
        """
        # Verifica si el diccionario de reemplazo está vacío
        if not cols_to_rename:
            logger.info(
                "El diccionario de columnas a renombrar está vacío. Sin cambios."
            )
            return base  # Retorna el DataFrame original sin cambios

        try:
            base_renombrada = base.rename(columns=cols_to_rename, inplace=False)
            logger.info("Proceso de renombrar columnas satisfactorio.")
        except Exception as e:
            logger.critical(f"Proceso de renombrar columnas fallido. Error: {e}")
            raise

        return base_renombrada

    def duplicar_columnas_pd(df: pd.DataFrame, mapeo_columnas: dict):
        """
        Duplica múltiples columnas en un DataFrame de pandas, asignándoles nuevos nombres.

        Parámetros:
        - df (pandas.DataFrame): El DataFrame original.
        - mapeo_columnas (dict): Un diccionario que mapea los nombres de las columnas existentes
                                a los nuevos nombres. Las claves son los nombres de las columnas
                                existentes y los valores son los nuevos nombres de columna.

        Retorna:
        - Un nuevo DataFrame con las columnas duplicadas añadidas.
        """
        # Duplicar las columnas especificadas utilizando un bucle
        for columna_original, columna_nueva in mapeo_columnas.items():
            df[columna_nueva] = df[columna_original]

        return df

    def Reemplazar_columna_en_funcion_de_otra(
        df: pd.DataFrame,
        nom_columna_a_reemplazar: str,
        nom_columna_de_referencia: str,
        mapeo: dict,
    ) -> pd.DataFrame:
        """
        Reemplaza los valores en una columna en función de los valores en otra columna en un DataFrame.

        Args:
            df (pandas.DataFrame): El DataFrame en el que se realizarán los reemplazos.
            columna_a_reemplazar (str): El nombre de la columna que se reemplazará.
            columna_de_referencia (str): El nombre de la columna que se utilizará como referencia para el reemplazo.
            mapeo (dict): Un diccionario que mapea los valores de la columna de referencia a los nuevos valores.

        Returns:
            pandas.DataFrame: El DataFrame actualizado con los valores reemplazados en la columna indicada.
        """
        try:
            logger.info(
                f"Inicio de remplazamiento de datos en {nom_columna_a_reemplazar}"
            )
            df[nom_columna_a_reemplazar] = np.where(
                df[nom_columna_de_referencia].isin(mapeo.keys()),
                df[nom_columna_de_referencia].map(mapeo),
                df[nom_columna_a_reemplazar],
            )
            logger.success(
                f"Proceso de remplazamiento en {nom_columna_a_reemplazar} exitoso"
            )
        except Exception as e:
            logger.critical(
                f"Proceso de remplazamiento de datos en {nom_columna_a_reemplazar} fallido."
            )
            raise e

        return df

    def Reemplazar_valores_con_dict_pd(
        df: pd.DataFrame, columna: str, diccionario_mapeo: dict
    ):
        """
        Reemplaza los valores en la columna especificada de un DataFrame según un diccionario de mapeo.

        Args:
        - df (pd.DataFrame): El DataFrame a modificar.
        - columna (str): El nombre de la columna que se va a reemplazar.
        - diccionario_mapeo (dict): Un diccionario que define la relación de mapeo de valores antiguos a nuevos.

        Returns:
        - pd.DataFrame: El DataFrame modificado con los valores de la columna especificada reemplazados.

        - TypeError: Si 'df' no es un DataFrame de pandas o 'diccionario_mapeo' no es un diccionario.
        - KeyError: Si la 'columna' especificada no se encuentra en el DataFrame.

        """
        try:
            # Verificar si la entrada es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

            # Verificar si la columna especificada existe en el DataFrame
            if columna not in df.columns:
                raise KeyError(f"Columna '{columna}' no encontrada en el DataFrame.")

            # Verificar si el diccionario de mapeo es un diccionario
            if not isinstance(diccionario_mapeo, dict):
                raise TypeError("'diccionario_mapeo' debe ser un diccionario.")

            df_copy = df.copy()
            # Realizar el reemplazo según el diccionario de mapeo manteniendo los valores no mapeados intactos
            df_copy.loc[:, columna] = df_copy[columna].replace(diccionario_mapeo)
            # Alternativa de reemplazo
            # df[columna] = df[columna].map(diccionario_mapeo).fillna(df[columna])

            # Registrar mensaje de éxito
            logger.success(
                f"Valores de la columna '{columna}' reemplazados según el diccionario de mapeo."
            )

            return df_copy

        except Exception as e:
            # Registrar mensaje crítico con detalles del tipo de error
            logger.critical(
                f"Error durante el reemplazo de valores en la columna. Tipo de error: {type(e).__name__}. Detalles: {str(e)}"
            )
            return None

    def Eliminar_columnas_pd(
        df: pd.DataFrame, columnas_a_eliminar: list
    ) -> pd.DataFrame:
        """
        Elimina las columnas especificadas de un DataFrame de pandas.

        Args:
            - df (pd.DataFrame): El DataFrame de pandas original.
            - columnas_a_eliminar (list): Lista de nombres de columnas a eliminar.

        Returns:
            pd.DataFrame: Un nuevo DataFrame sin las columnas especificadas.
        """
        try:
            # Eliminar las columnas del DataFrame
            df_resultado = df.drop(columns=columnas_a_eliminar)

            # Registrar información sobre las columnas eliminadas
            logger.info(f"Columnas {columnas_a_eliminar} eliminadas con éxito.")

            return df_resultado
        except Exception as e:
            # Registrar un error crítico si ocurre una excepción
            #logger.critical(f"Error al eliminar columnas: {str(e)}")
            # Propagar la excepción para que el usuario sea consciente del problema
            raise e

    def Agregar_columna_constante(
        dataframe: pd.DataFrame, nombre_columna: str | list, valor_constante: Any
    ) -> pd.DataFrame | None:
        """
        Añade una nueva columna con un valor constante a un DataFrame.

        Args:
            dataframe (pd.DataFrame): DataFrame al que se añadirá la nueva columna.
            nombre_columna (str): Nombre de la nueva columna, o lista de columnas a agregar.
            valor_constante (Any): Valor constante que se asignará a todas las filas de la columna.

        Returns:
            Union[pd.DataFrame, None]: DataFrame con la nueva columna añadida o None si ocurre un error.
        """
        try:
            # Verificar si dataframe es un DataFrame de pandas
            if not isinstance(dataframe, pd.DataFrame):
                raise TypeError(
                    "El argumento 'dataframe' debe ser un DataFrame de pandas."
                )

            # Crear una copia del dataframe.
            df = dataframe.copy()
            # Añadir la nueva columna con el valor constante
            df[nombre_columna] = valor_constante

            # Registrar el evento
            # logger.info(
            #    f"Se añadió la columna '{nombre_columna}' con el valor constante '{valor_constante}' al DataFrame."
            # )

            return df

        except Exception as e:
            logger.critical(
                f"Error inesperado al añadir columna con valor constante: {str(e)}"
            )
            return None

    def Agregar_multiples_cols_constantes(df: pd.DataFrame, dict_cols: dict):
        """
        Añade mutliples columnas con valores contantes de cualquier tipo, por medio de un diccionario de mapeo.

        Args:
            df (pd.DataFrame): DataFrame al que se añadirá la nueva columna.
            dict_cols ( dict): Diccionario que contiene las columnas.
              - keys ( Nombre de cada columna a agregar al dataframe).
              - Values ( Valor constante de tipo int|str|float|bool)

        Returns:
            [pd.DataFrame, None]: DataFrame con la nueva columna añadida o None si ocurre un error.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

            # Intenta agregar las nuevas columnas usando assign
            df_modificado = df.assign(**dict_cols)
            return df_modificado

        except Exception as e:
            error_message = f"Error: {type(e).__name__}, {str(e)}"
            raise (error_message, df)

    @Registro_tiempo
    def concatenar_dataframes(df_list: list[pd.DataFrame]):
        """
        Concatena una lista de DataFrames.

        Args:
            df_list: Lista de DataFrames a concatenar.

        Returns:
            Un DataFrame concatenado.
        """
        try:
            if len(df_list) != 1:
                #logger.info("Inicio concatenacion de dataframes")
                concatenados = pd.concat(df_list, ignore_index=True)
                #logger.success("se concatenaron los dataframes correctamente")
                return concatenados
            else:
                return df_list[0]
        except Exception as e:
            logger.critical(e)
            raise e

    def duplicar_filas_segun_diccionario(
        df: pd.DataFrame,
        columna_clave: str,
        dict_listas: dict,
        nombre_nueva_columna: str,
    ):
        """
        Duplica las filas de un DataFrame según los valores de un diccionario y agrega una nueva columna
        con los elementos de la lista correspondiente en el diccionario.

        Parámetros:
        df (pd.DataFrame): El DataFrame original.
        columna_clave (str): El nombre de la columna que se usará como clave en el diccionario.
        dict_listas (dict): El diccionario donde las claves corresponden a los valores de la columna clave,
                            y los valores son listas de elementos para duplicar las filas.
        nombre_nueva_columna (str): El nombre de la nueva columna que contendrá los elementos de la lista.

        Retorna:
        pd.DataFrame: Un nuevo DataFrame con las filas duplicadas y la nueva columna.
        """
        # Crear una lista para almacenar los DataFrames resultantes
        df_list = []
        # Iterar sobre cada fila y duplicar según el diccionario
        for valor_clave in df[columna_clave].unique():
            if valor_clave in dict_listas:
                # Filtrar las filas que corresponden a la clave actual
                df_filtro = df[df[columna_clave] == valor_clave]
                # Crear el DataFrame replicado
                df_replicado = pd.DataFrame(
                    np.repeat(df_filtro.values, len(dict_listas[valor_clave]), axis=0),
                    columns=df.columns,
                )
                # Añadir la nueva columna con los valores de la lista del diccionario
                df_replicado[nombre_nueva_columna] = dict_listas[valor_clave] * len(
                    df_filtro
                )
                # Añadir el DataFrame replicado a la lista
                df_list.append(df_replicado)

        # Concatenar todos los DataFrames en uno solo

        if len(df_list) > 0:
            df_resultante = pd.concat(df_list, ignore_index=True)
        return df_resultante
        
