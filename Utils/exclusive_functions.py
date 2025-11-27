from sqlalchemy import create_engine
import pandas as pd
from loguru import logger 


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


class ProcesarInsumoCxsDB:
    def __init__(self, config):
        # Inicializa la conexión a la base de datos usando la URI de configuración
        self.engine = create_engine(config["uri"])
        self.config_db = config
        
    def build_where_clause(self):
        
        # Construye la cláusula WHERE de la consulta SQL
        tipo = self.config_db["query"]["where"]["tipo"]
        centro_costo = self.config_db["query"]["where"]["centro_costo"]
        oficina_ventas = List_to_sql(self.config_db["query"]["where"]["oficina_ventas"])
        sector = List_to_sql(self.config_db["query"]["where"]["sector"])
        where_clause = f"""
        WHERE
            Tipo = '{tipo}' AND
            Centro_Costo <> '{centro_costo}' AND
            Oficina_ventas IN ({oficina_ventas}) AND
            Sector IN ({sector})
        """
        return where_clause
    
    def build_query(self):
        # Construye la consulta SQL utilizando los parámetros de configuración
        logger.info("Construyendo la consulta SQL completa...")
        select_clause = self.config_db["query"]["select"]
        from_clause = self.config_db["query"]["from"]
        where_clause = self.build_where_clause()
        group_by_clause = self.config_db["query"]["group_by"]
        order_by_clause = self.config_db["query"]["order_by"]

        query = f"""
        SELECT {select_clause}
        FROM {from_clause}
        {where_clause}
        GROUP BY {group_by_clause}
        ORDER BY {order_by_clause}
        """
        return query


    
    def execute_query(self):
        logger.info("Ejecutando consulta SQL...")
        # Construye y ejecuta la consulta SQL
        query = self.build_query()
        df = pd.read_sql(query, self.engine)
        logger.success("Consulta SQL cargada...")
        return df

