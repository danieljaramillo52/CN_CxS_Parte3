## Funciones básicas - Generales del proyecto_CxS Parte3
import pandas as pd
import os
import time
import inspect
import yaml
import time
from typing import Dict
from loguru import logger
from pathlib import Path
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import openpyxl


def Registro_tiempo(original_func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = original_func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(
            f"Tiempo de ejecución de {original_func.__name__}: {execution_time} segundos"
        )
        return result

    return wrapper


class ErrorHandler:
    @staticmethod
    def log_error(e, message):
        # Usamos inspect para capturar el marco de la llamada actual
        current_frame = inspect.currentframe()
        # Vamos dos niveles arriba para capturar el punto desde donde fue llamada la función 'seleccionar_columnas_pd'
        call_frame = inspect.getouterframes(current_frame, 2)[2]
        logger.critical(
            f"{message} - Error occurred in file {call_frame.filename}, line {call_frame.lineno}"
        )


def Obtener_lugar_de_ejecucion() -> str:
    """
    Captura la respuesta del usuario sobre el lugar de ejecución y ajusta la ruta actual si es necesario.

    Returns:
        str: La respuesta del usuario, validada para ser 'si' o 'no'.
    """
    while True:
        lugar_de_ejecucion = (
            input(
                "¿Está ejecutando esta automatización desde Python IDLE ó desde cmd?: (si/no): "
            )
            .strip()
            .lower()
        )
        if lugar_de_ejecucion in ["si", "no"]:
            break
        else:
            print("Respuesta no válida. Por favor, ingrese 'si' o 'no'.")

    if lugar_de_ejecucion == "si":
        ruta_actual = os.getcwd()
        ruta_padre = os.path.dirname(ruta_actual)
        os.chdir(ruta_padre)

    return lugar_de_ejecucion


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
            raise ValueError("Las columnas especificadas no existen en el DataFrame.")

        if col_clave == col_valor:
            resultado_dict = df[col_clave].to_dict()
        else:
            resultado_dict = df.set_index(col_clave)[col_valor].to_dict()
        return resultado_dict

    except ValueError as ve:
        # Registrar un mensaje crítico si hay un error
        logger.critical(f"Error: {ve}")
        raise ve


def Procesar_configuracion(nom_archivo_configuracion: str) -> dict:
    """Lee un archivo YAML de configuración para un proyecto.

    Args:
        nom_archivo_configuracion (str): Nombre del archivo YAML que contiene
            la configuración del proyecto.

    Returns:
        dict: Un diccionario con la información de configuración leída del archivo YAML.
    """
    try:
        with open(nom_archivo_configuracion, "r", encoding="utf-8") as archivo:
            configuracion_yaml = yaml.safe_load(archivo)
        logger.success("Proceso de obtención de configuración satisfactorio")
    except Exception as e:
        logger.critical(f"Proceso de lectura de configuración fallido {e}")
        raise e

    return configuracion_yaml


class ExcelReader:
    def __init__(self, path: str):
        self.path = path

    @Registro_tiempo
    def Lectura_insumos_excel(
        self, nom_insumo: str, nom_hoja: str, cols: int | list, skiprows: int
    ) -> pd.DataFrame:
        """
        Lee archivos de Excel especificando hoja y columnas.
        """
        if isinstance(cols, list):
            range_cols = cols
        else:
            range_cols = list(range(cols))

        try:
            logger.info(f"Inicio lectura {nom_insumo} Hoja: {nom_hoja}")
            base_leida = pd.read_excel(
                self.path + nom_insumo,
                sheet_name=nom_hoja,
                skiprows=skiprows,
                usecols=range_cols,
                dtype=str,
                engine="openpyxl",
            )
            logger.success(
                f"Lectura de {nom_insumo} Hoja: {nom_hoja} realizada con éxito"
            )
            return base_leida
        except Exception as e:
            logger.error(f"Proceso de lectura fallido: {e}")
            raise Exception(f"Error al leer el archivo: {e}")

    @Registro_tiempo
    def Lectura_simple_excel(self, nom_insumo: str, nom_hoja: str) -> pd.DataFrame:
        """
        Lee un archivo de Excel únicamente utilizando el nombre de su hoja sin parámetros adicionales.
        """
        try:
            logger.info(f"Inicio lectura simple de {nom_insumo}")
            base_leida = pd.read_excel(
                self.path + nom_insumo,
                sheet_name=nom_hoja,
                dtype=str,
            )
            logger.success(f"Lectura simple de {nom_insumo} realizada con éxito")
            return base_leida
        except Exception as e:
            logger.error(f"Proceso de lectura fallido: {e}")
            raise Exception(f"Error al leer el archivo: {e}")


def List_to_sql(values: list[str]):
    """
    Convierte una lista de valores en una cadena de valores SQL correctamenteformateada.
    Parameters:
    values (list of str): Lista de valores a convertir. Cada valor en la lista debeser una cadena (str).
    Returns:
    str: Una cadena de valores SQL separada por comas y entre comillas simples.
    Raises:
    TypeError: Si algún elemento de la lista no es una cadena (str).
    ValueError: Si la lista está vacía.
    """
    if not values:
        raise ValueError("La lista de valores no puede estar vacía.")
    for value in values:
        if not isinstance(value, str):
            raise TypeError(
                f"Todos los elementos de la lista deben ser cadenas (str). Elementoinválido: {value}"
            )
    return ", ".join(f"'{value}'" for value in values)


def crear_dict_col_llave_col_valores(df, columna_clave, columna_valores):
    """
    Crea un diccionario donde cada clave es un elemento único de una columna,
    y cada valor es una lista con los elementos correspondientes de otra columna.

    Parámetros:
    df (pd.DataFrame): El DataFrame del cual se extraerán los datos.
    columna_clave (str): El nombre de la columna que se usará como claves del diccionario.
    columna_valores (str): El nombre de la columna que se usará para los valores del diccionario.

    Retorna:
    dict: Un diccionario con las claves y valores especificados.
    """
    diccionario = {}
    for clave in df[columna_clave].unique():
        diccionario[clave] = (
            df.loc[df[columna_clave] == clave, columna_valores].unique().tolist()
        )
    return diccionario


class DatabaseManager:
    """
    Clase para gestionar la conexión y operaciones con PostgreSQL
    utilizando psycopg2 y SQLAlchemy.

    Esta clase permite establecer conexiones a la base de datos,
    crear motores de conexión con SQLAlchemy y realizar inserciones de datos en tablas.
    """

    def __init__(self):
        """
        Inicializa el gestor de base de datos cargando las variables de entorno
        y configurando los parámetros de conexión.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        dotenv_path = os.path.dirname(current_dir)

        # Cargar el .env desde la ruta especificada
        load_dotenv(dotenv_path=dotenv_path + "\.env")
        # Carga las variables de entorno desde un archivo .env

        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")

    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Establece y devuelve una conexión a la base de datos PostgreSQL.

        Returns:
            psycopg2.extensions.connection: Objeto de conexión a la base de datos.

        Raises:
            psycopg2.DatabaseError: Si ocurre un error en la conexión.
        """
        try:
            return psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
        except psycopg2.DatabaseError as e:
            raise Exception(f"Error al conectar con la base de datos: {e}")

    def create_engine(self):
        """
        Crea y devuelve un motor SQLAlchemy para interactuar con la base de datos.

        Returns:
            sqlalchemy.engine.base.Engine: Motor de conexión SQLAlchemy.
        """
        return create_engine("postgresql+psycopg2://", creator=self.get_connection)

    def insert_dataframe(
        self, dataframe: pd.DataFrame, table_name: str, schema: str = "public"
    ) -> None:
        """
        Inserta un DataFrame en una tabla específica de la base de datos.

        Args:
            dataframe (pd.DataFrame): DataFrame con los datos a insertar.
            table_name (str): Nombre de la tabla en la que se insertarán los datos.
            schema (str, optional): Esquema en el que se encuentra la tabla. Por defecto es 'public'.

        Raises:
            ValueError: Si el DataFrame está vacío.
            Exception: Si ocurre un error en la inserción de datos.
        """
        if dataframe.empty:
            raise ValueError(
                "El DataFrame está vacío y no se puede insertar en la base de datos."
            )

        try:
            engine = self.create_engine()
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                schema=schema,
                index=False,
            )
        except Exception as e:
            raise Exception(f"Error al insertar datos en la tabla {table_name}: {e}")

    def read_table(self, table_name: str, schema: str = "public") -> pd.DataFrame:
        """
        Lee una tabla de PostgreSQL y la devuelve como un DataFrame de pandas.

        Args:
            table_name (str): Nombre de la tabla que se desea leer.
            schema (str, optional): Esquema en el que se encuentra la tabla. Por defecto es 'public'.

        Returns:
            pd.DataFrame: DataFrame con los datos de la tabla.

        Raises:
            Exception: Si ocurre un error al leer los datos.
        """
        try:
            engine = self.create_engine()
            query = f'SELECT * FROM "{schema}"."{table_name}"'  # Consulta SQL segura
            df = pd.read_sql(query, con=engine)
            return df
        except Exception as e:
            raise Exception(f"Error al leer la tabla {table_name}: {e}")
