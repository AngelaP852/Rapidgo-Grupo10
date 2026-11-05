"""
Módulo de funciones de validación de calidad de datos para RapidGo - Variante 1 (SLA de entregas).
Contiene las funciones validar_raw y validar_curated requeridas para la Semana 3.
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validar_raw(data_list) -> bool:
    """
    Valida los datos provenientes de la capa Raw.
    Verifica los campos obligatorios de la Variante 1:
    - pedido, timestamps, zona, distancia, estado
    """
    if not isinstance(data_list, list):
        logger.error("Error en validar_raw: La entrada no es una lista de registros.")
        return False

    if len(data_list) == 0:
        logger.warning("Advertencia en validar_raw: La lista de datos crudos está vacía.")
        return True

    campos_requeridos = ["pedido", "timestamps", "zona", "distancia", "estado"]
    
    for idx, record in enumerate(data_list):
        for campo in campos_requeridos:
            if campo not in record or record[campo] is None:
                logger.error(f"Fallo de calidad en RAW [Registro {idx}]: Falta o es nulo el campo '{campo}'.")
                return False
                
    logger.info("Validación RAW exitosa: Todos los registros cumplen con el contrato básico.")
    return True


def validar_curated(data_list) -> bool:
    """
    Valida los datos procesados en la capa Curated (métricas y KPIs de SLA).
    """
    if not isinstance(data_list, list):
        logger.error("Error en validar_curated: La entrada no es una lista.")
        return False

    for idx, record in enumerate(data_list):
        if "tiempo_promedio" in record and record["tiempo_promedio"] < 0:
            logger.error(f"Fallo en CURATED [Registro {idx}]: tiempo_promedio negativo.")
            return False
            
        if "porcentaje_fuera_sla" in record:
            pct = record["porcentaje_fuera_sla"]
            if not (0.0 <= pct <= 100.0):
                logger.error(f"Fallo en CURATED [Registro {idx}]: porcentaje_fuera_sla ({pct}) fuera de rango.")
                return False

    logger.info("Validación CURATED exitosa: Métricas de SLA consistentes.")
    return True