"""Modulo que configura las rutas de las carpetas principales del proyecto. (Scripts / Utils) Cumple la función de que todo sea visible y accesible desde main.py"""

import sys
import os

# Obtener el directorio del script actual
current_dir = os.path.dirname(os.path.abspath(__file__))

list_dir = ["Scripts", "Utils"]

# Obtener el directorio del script actual
current_dir = os.path.dirname(os.path.abspath(__file__))

# Obtener el directorio padre
parent_dir = os.path.dirname(current_dir)

for cada_path in list_dir:
    # Obtener los path  de los directorios adicionales
    parent_dir = os.path.dirname(current_dir)
    path_agregar = os.path.join(parent_dir, cada_path)

    # Añadir al sys.path si no están ya
    if path_agregar not in sys.path:
        sys.path.append(path_agregar)
