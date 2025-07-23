"""
Utilidades y funciones auxiliares para el proyecto ETL.
"""

import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


def setup_project_directories() -> None:
    """Crea la estructura de directorios del proyecto si no existe."""
    directories = [
        "data/raw",
        "data/processed",
        "logs",
        "config",
        "tests",
        "docs",
        "notebooks"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Directorio creado/verificado: {directory}")


def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Valida que el rango de fechas sea válido.
    
    Args:
        start_date: Fecha de inicio en formato YYYY-MM-DD
        end_date: Fecha de fin en formato YYYY-MM-DD
        
    Returns:
        True si el rango es válido, False en caso contrario
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        return start <= end
    except ValueError:
        return False


def get_file_size_mb(file_path: Union[str, Path]) -> float:
    """
    Obtiene el tamaño de un archivo en MB.
    
    Args:
        file_path: Ruta al archivo
        
    Returns:
        Tamaño del archivo en MB
    """
    try:
        size_bytes = Path(file_path).stat().st_size
        return size_bytes / (1024 * 1024)
    except FileNotFoundError:
        return 0.0


def save_json_report(data: Dict[str, Any], output_path: Union[str, Path]) -> None:
    """
    Guarda un diccionario como archivo JSON con formato.
    
    Args:
        data: Datos a guardar
        output_path: Ruta del archivo de salida
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"Reporte JSON guardado en: {output_path}")


def format_duration(seconds: float) -> str:
    """
    Formatea una duración en segundos a un string legible.
    
    Args:
        seconds: Duración en segundos
        
    Returns:
        String formateado (ej: "2m 30s", "1h 15m 20s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.0f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        return f"{hours}h {remaining_minutes}m {remaining_seconds:.0f}s"


def get_spark_ui_url() -> Optional[str]:
    """
    Obtiene la URL de la interfaz web de Spark si está disponible.
    
    Returns:
        URL de Spark UI o None si no está disponible
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            return spark.sparkContext.uiWebUrl
    except:
        pass
    return None


class PerformanceTimer:
    """Context manager para medir tiempo de ejecución."""
    
    def __init__(self, operation_name: str = "Operation"):
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        logger.info(f"Iniciando: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        if exc_type is None:
            logger.info(f"Completado: {self.operation_name} en {format_duration(duration)}")
        else:
            logger.error(f"Error en: {self.operation_name} después de {format_duration(duration)}")
    
    @property
    def duration_seconds(self) -> float:
        """Retorna la duración en segundos."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


class DataQualityChecker:
    """Utilidades para verificar calidad de datos."""
    
    @staticmethod
    def check_completeness(df, required_columns: List[str]) -> Dict[str, float]:
        """
        Verifica completitud de columnas requeridas.
        
        Args:
            df: DataFrame de PySpark
            required_columns: Lista de columnas requeridas
            
        Returns:
            Dict con porcentaje de completitud por columna
        """
        from pyspark.sql.functions import col, count, when, isnan, isnull
        
        total_rows = df.count()
        completeness = {}
        
        for column in required_columns:
            if column in df.columns:
                non_null_count = df.select(
                    count(when(
                        col(column).isNotNull() & 
                        ~isnan(col(column)) & 
                        (col(column) != ""),
                        column
                    )).alias('non_null_count')
                ).collect()[0]['non_null_count']
                
                completeness[column] = (non_null_count / total_rows) * 100 if total_rows > 0 else 0
            else:
                completeness[column] = 0.0
        
        return completeness
    
    @staticmethod
    def detect_outliers_iqr(df, numeric_columns: List[str]) -> Dict[str, Dict]:
        """
        Detecta outliers usando el método IQR.
        
        Args:
            df: DataFrame de PySpark
            numeric_columns: Lista de columnas numéricas
            
        Returns:
            Dict con información de outliers por columna
        """
        from pyspark.sql.functions import col, count, when, percentile_approx
        
        outliers_info = {}
        
        for column in numeric_columns:
            if column not in df.columns:
                continue
            
            # Calcular percentiles
            percentiles = df.select(
                percentile_approx(col(column), 0.25).alias('q1'),
                percentile_approx(col(column), 0.75).alias('q3')
            ).collect()[0]
            
            q1, q3 = percentiles['q1'], percentiles['q3']
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Contar outliers
            outlier_count = df.filter(
                (col(column) < lower_bound) | (col(column) > upper_bound)
            ).count()
            
            total_count = df.filter(col(column).isNotNull()).count()
            
            outliers_info[column] = {
                'outlier_count': outlier_count,
                'total_count': total_count,
                'outlier_percentage': (outlier_count / total_count) * 100 if total_count > 0 else 0,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'q1': q1,
                'q3': q3,
                'iqr': iqr
            }
        
        return outliers_info


class ConfigValidator:
    """Validador de configuraciones."""
    
    @staticmethod
    def validate_file_paths(config: Dict[str, Any]) -> List[str]:
        """
        Valida que las rutas de archivos en la configuración existan.
        
        Args:
            config: Diccionario de configuración
            
        Returns:
            Lista de errores encontrados
        """
        errors = []
        
        # Validar archivo de entrada
        input_path = config.get('data', {}).get('input', {}).get('file_path')
        if input_path and not Path(input_path).exists():
            errors.append(f"Archivo de entrada no encontrado: {input_path}")
        
        return errors
    
    @staticmethod
    def validate_date_filters(config: Dict[str, Any]) -> List[str]:
        """
        Valida los filtros de fecha en la configuración.
        
        Args:
            config: Diccionario de configuración
            
        Returns:
            Lista de errores encontrados
        """
        errors = []
        
        filters = config.get('data', {}).get('filters', {})
        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        
        if start_date and end_date:
            if not validate_date_range(start_date, end_date):
                errors.append(f"Rango de fechas inválido: {start_date} - {end_date}")
        
        return errors


def create_sample_data_summary(df) -> Dict[str, Any]:
    """
    Crea un resumen de muestra de datos para logging.
    
    Args:
        df: DataFrame de PySpark
        
    Returns:
        Dict con resumen de datos
    """
    try:
        summary = {
            'total_rows': df.count(),
            'total_columns': len(df.columns),
            'columns': df.columns,
            'sample_data': []
        }
        
        # Obtener muestra de datos (máximo 3 filas)
        sample_rows = df.limit(3).collect()
        for row in sample_rows:
            summary['sample_data'].append(row.asDict())
        
        return summary
    
    except Exception as e:
        logger.warning(f"Error creando resumen de datos: {e}")
        return {'error': str(e)}


def cleanup_spark_checkpoints(checkpoint_dir: str = "spark-warehouse") -> None:
    """
    Limpia archivos de checkpoint de Spark.
    
    Args:
        checkpoint_dir: Directorio de checkpoints
    """
    try:
        import shutil
        checkpoint_path = Path(checkpoint_dir)
        if checkpoint_path.exists():
            shutil.rmtree(checkpoint_path)
            logger.info(f"Checkpoints de Spark limpiados: {checkpoint_path}")
    except Exception as e:
        logger.warning(f"Error limpiando checkpoints: {e}")


def get_system_info() -> Dict[str, Any]:
    """
    Obtiene información del sistema para debugging.
    
    Returns:
        Dict con información del sistema
    """
    import platform
    import psutil
    
    try:
        return {
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'cpu_count': psutil.cpu_count(),
            'memory_gb': round(psutil.virtual_memory().total / (1024**3), 2),
            'disk_free_gb': round(psutil.disk_usage('.').free / (1024**3), 2)
        }
    except Exception as e:
        return {'error': f"Error obteniendo info del sistema: {e}"}


class DataFrameProfiler:
    """Profiler básico para DataFrames de PySpark."""
    
    def __init__(self, df):
        self.df = df
    
    def profile(self) -> Dict[str, Any]:
        """
        Genera un perfil básico del DataFrame.
        
        Returns:
            Dict con perfil de datos
        """
        from pyspark.sql.functions import col, count, countDistinct, min as spark_min, max as spark_max
        
        profile = {
            'basic_stats': {
                'row_count': self.df.count(),
                'column_count': len(self.df.columns),
                'columns': self.df.columns
            },
            'column_profiles': {}
        }
        
        for column in self.df.columns:
            col_type = dict(self.df.dtypes)[column]
            
            col_profile = {
                'data_type': col_type,
                'null_count': self.df.filter(col(column).isNull()).count(),
                'distinct_count': self.df.select(countDistinct(col(column))).collect()[0][0]
            }
            
            # Estadísticas adicionales para columnas numéricas
            if col_type in ['int', 'bigint', 'float', 'double']:
                stats = self.df.select(
                    spark_min(col(column)).alias('min'),
                    spark_max(col(column)).alias('max')
                ).collect()[0]
                
                col_profile.update({
                    'min_value': stats['min'],
                    'max_value': stats['max']
                })
            
            profile['column_profiles'][column] = col_profile
        
        return profile


# Constantes útiles
COUNTRY_CODES = {
    'GT': 'Guatemala',
    'PE': 'Perú',
    'EC': 'Ecuador',
    'SV': 'El Salvador',
    'HN': 'Honduras',
    'JM': 'Jamaica'
}

DELIVERY_TYPE_DESCRIPTIONS = {
    'ZPRE': 'Entrega de Rutina Pre-autorizada',
    'ZVE1': 'Entrega de Rutina Verificada',
    'Z04': 'Entrega con Bonificación Tipo 4',
    'Z05': 'Entrega con Bonificación Tipo 5',
    'COBR': 'Entrega contra Reembolso (Excluida)'
}

UNIT_DESCRIPTIONS = {
    'CS': 'Cajas (1 CS = 20 unidades)',
    'ST': 'Unidades individuales'
}


if __name__ == "__main__":
    # Ejemplo de uso de las utilidades
    setup_project_directories()
    
    # Ejemplo de timer
    with PerformanceTimer("Operación de ejemplo"):
        import time
        time.sleep(2)
    
    # Ejemplo de validación de fechas
    print("Validación de fechas:", validate_date_range("2025-01-01", "2025-12-31"))
    
    # Información del sistema
    print("Info del sistema:", get_system_info())