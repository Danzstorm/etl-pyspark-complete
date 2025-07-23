"""
Procesador principal de datos para el ETL de entregas de productos.
Implementa todas las transformaciones y reglas de negocio especificadas.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, isnan, isnull,
    to_date, date_format, current_timestamp, round as spark_round,
    regexp_extract, length, trim, upper, coalesce, sum as spark_sum,
    count, mean, stddev, monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)

from config_manager import ConfigManager

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityException(Exception):
    """Excepción personalizada para problemas de calidad de datos."""
    pass


class DataProcessor:
    """
    Procesador principal de datos para entregas de productos.
    
    Implementa un pipeline ETL completo con validación de datos,
    transformaciones configurables y generación de reportes de calidad.
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Inicializa el procesador de datos.
        
        Args:
            config_manager: Gestor de configuración del proyecto
        """
        self.config = config_manager
        self.spark: Optional[SparkSession] = None
        self.input_df: Optional[DataFrame] = None
        self.processed_df: Optional[DataFrame] = None
        self.quality_report: Dict = {}
        
        self._init_spark()
    
    def _init_spark(self) -> None:
        """Inicializa la sesión de Spark con configuración personalizada."""
        try:
            spark_config = self.config.get_spark_config()
            
            builder = SparkSession.builder.appName(spark_config['app_name'])
            
            # Aplicar configuraciones adicionales
            for key, value in spark_config.get('config', {}).items():
                builder = builder.config(key, value)
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark inicializado: {spark_config['app_name']}")
            
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            raise
    
    def _define_schema(self) -> StructType:
        """Define el schema esperado para el DataFrame de entrada."""
        return StructType([
            StructField("pais", StringType(), True),
            StructField("fecha_proceso", StringType(), True),
            StructField("transporte", StringType(), True),
            StructField("ruta", StringType(), True),
            StructField("tipo_entrega", StringType(), True),
            StructField("material", StringType(), True),
            StructField("precio", DoubleType(), True),
            StructField("cantidad", DoubleType(), True),
            StructField("unidad", StringType(), True)
        ])
    
    def load_data(self) -> DataFrame:
        """
        Carga los datos desde el archivo especificado en la configuración.
        
        Returns:
            DataFrame con los datos cargados
        """
        try:
            data_config = self.config.get_data_config()
            input_config = data_config['input']
            
            file_path = input_config['file_path']
            
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            # Cargar datos con schema definido
            self.input_df = (
                self.spark.read
                .option("header", input_config['header'])
                .option("delimiter", input_config['delimiter'])
                .option("encoding", input_config['encoding'])
                .option("inferSchema", "false")  # Usar schema definido
                .schema(self._define_schema())
                .csv(file_path)
            )
            
            # Generar reporte inicial de calidad
            self._generate_initial_quality_report()
            
            logger.info(f"Datos cargados exitosamente: {self.input_df.count()} registros")
            return self.input_df
            
        except Exception as e:
            logger.error(f"Error cargando datos: {e}")
            raise
    
    def _generate_initial_quality_report(self) -> None:
        """Genera un reporte inicial de calidad de datos."""
        if not self.input_df:
            return
        
        total_records = self.input_df.count()
        
        self.quality_report = {
            'initial_records': total_records,
            'columns_with_nulls': {},
            'empty_materials': 0,
            'zero_prices': 0,
            'negative_quantities': 0,
            'invalid_dates': 0,
            'unknown_delivery_types': 0,
            'data_quality_issues': []
        }
        
        # Contar nulos por columna
        for col_name in self.input_df.columns:
            null_count = self.input_df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                self.quality_report['columns_with_nulls'][col_name] = null_count
        
        # Detectar problemas específicos
        self.quality_report['empty_materials'] = (
            self.input_df.filter(
                col('material').isNull() | 
                (col('material') == "") |
                (trim(col('material')) == "")
            ).count()
        )
        
        # Precios con notación científica o cero
        self.quality_report['zero_prices'] = (
            self.input_df.filter(
                col('precio').isNull() |
                (col('precio') == 0) |
                col('precio').rlike(r'^\d+E-\d+$')
            ).count()
        )
        
        self.quality_report['negative_quantities'] = (
            self.input_df.filter(col('cantidad') < 0).count()
        )
        
        logger.info(f"Reporte de calidad inicial generado: {self.quality_report}")
    
    def apply_filters(self) -> DataFrame:
        """
        Aplica filtros configurables al DataFrame.
        
        Returns:
            DataFrame filtrado
        """
        if not self.input_df:
            raise ValueError("Datos no cargados. Ejecutar load_data() primero.")
        
        df = self.input_df
        filters_config = self.config.get('data.filters')
        
        # Convertir fecha_proceso a formato de fecha
        df = df.withColumn(
            'fecha_proceso_date',
            to_date(col('fecha_proceso'), 'yyyyMMdd')
        )
        
        # Filtro por rango de fechas
        start_date = filters_config['start_date']
        end_date = filters_config['end_date']
        
        df = df.filter(
            (col('fecha_proceso_date') >= lit(start_date)) &
            (col('fecha_proceso_date') <= lit(end_date))
        )
        
        logger.info(f"Filtro de fechas aplicado: {start_date} a {end_date}")
        
        # Filtro por países
        countries = filters_config.get('countries')
        if countries:
            df = df.filter(col('pais').isin(countries))
            logger.info(f"Filtro de países aplicado: {countries}")
        
        # Filtro por tipos de entrega válidos
        delivery_config = self.config.get('transformations.delivery_types')
        valid_types = delivery_config['routine'] + delivery_config['bonus']
        df = df.filter(col('tipo_entrega').isin(valid_types))
        
        logger.info(f"Registros después de filtros: {df.count()}")
        return df
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Limpia los datos según las reglas configuradas.
        
        Args:
            df: DataFrame a limpiar
            
        Returns:
            DataFrame limpio
        """
        cleaning_config = self.config.get('transformations.data_cleaning')
        
        # Eliminar materiales vacíos
        if cleaning_config['remove_empty_materials']:
            initial_count = df.count()
            df = df.filter(
                col('material').isNotNull() &
                (col('material') != "") &
                (trim(col('material')) != "")
            )
            removed = initial_count - df.count()
            logger.info(f"Eliminados {removed} registros con material vacío")
        
        # Eliminar precios cero o en notación científica
        if cleaning_config['remove_zero_prices']:
            initial_count = df.count()
            df = df.filter(
                col('precio').isNotNull() &
                (col('precio') > 0) &
                ~col('precio').rlike(r'^\d+E-\d+$')
            )
            removed = initial_count - df.count()
            logger.info(f"Eliminados {removed} registros con precios inválidos")
        
        # Eliminar cantidades negativas
        if cleaning_config['remove_negative_quantities']:
            initial_count = df.count()
            df = df.filter(col('cantidad') > 0)
            removed = initial_count - df.count()
            logger.info(f"Eliminados {removed} registros con cantidades negativas")
        
        return df
    
    def transform_units(self, df: DataFrame) -> DataFrame:
        """
        Transforma las unidades según la configuración.
        
        Args:
            df: DataFrame a transformar
            
        Returns:
            DataFrame con unidades normalizadas
        """
        unit_config = self.config.get('transformations.unit_conversion')
        multiplier = unit_config['cs_to_units_multiplier']
        
        # Normalizar unidades: CS -> unidades (multiplicar por 20)
        df = df.withColumn(
            'cantidad_normalizada',
            when(upper(col('unidad')) == 'CS', col('cantidad') * multiplier)
            .otherwise(col('cantidad'))
        ).withColumn(
            'unidad_normalizada',
            lit(unit_config['target_unit'])
        )
        
        logger.info(f"Unidades normalizadas (CS * {multiplier} = unidades)")
        return df
    
    def categorize_deliveries(self, df: DataFrame) -> DataFrame:
        """
        Categoriza los tipos de entrega según la configuración.
        
        Args:
            df: DataFrame a categorizar
            
        Returns:
            DataFrame con categorías de entrega
        """
        delivery_config = self.config.get('transformations.delivery_types')
        
        # Crear columnas booleanas para cada tipo de entrega
        df = df.withColumn(
            'es_entrega_rutina',
            when(col('tipo_entrega').isin(delivery_config['routine']), lit(1)).otherwise(lit(0))
        ).withColumn(
            'es_entrega_bonificacion',
            when(col('tipo_entrega').isin(delivery_config['bonus']), lit(1)).otherwise(lit(0))
        ).withColumn(
            'categoria_entrega',
            when(col('tipo_entrega').isin(delivery_config['routine']), lit('RUTINA'))
            .when(col('tipo_entrega').isin(delivery_config['bonus']), lit('BONIFICACION'))
            .otherwise(lit('OTRO'))
        )
        
        logger.info("Categorización de entregas aplicada")
        return df
    
    def add_calculated_columns(self, df: DataFrame) -> DataFrame:
        """
        Añade columnas calculadas adicionales.
        
        Args:
            df: DataFrame base
            
        Returns:
            DataFrame con columnas adicionales
        """
        additional_config = self.config.get('transformations.additional_columns')
        
        # Valor total (precio * cantidad normalizada)
        if additional_config['total_value']:
            df = df.withColumn(
                'valor_total',
                spark_round(col('precio') * col('cantidad_normalizada'), 2)
            )
        
        # Fecha de procesamiento actual
        if additional_config['processing_date']:
            df = df.withColumn(
                'fecha_procesamiento',
                current_timestamp()
            )
        
        # Score de calidad de datos
        if additional_config['data_quality_score']:
            df = df.withColumn(
                'score_calidad_datos',
                when(col('material').isNotNull() & (col('material') != ""), 1).otherwise(0) +
                when(col('precio') > 0, 1).otherwise(0) +
                when(col('cantidad_normalizada') > 0, 1).otherwise(0) +
                when(col('pais').isNotNull(), 1).otherwise(0) +
                when(col('ruta').isNotNull(), 1).otherwise(0)
            )
        
        # Identificador único
        df = df.withColumn('id_registro', monotonically_increasing_id())
        
        # Año y mes de proceso para análisis
        df = df.withColumn(
            'año_proceso',
            date_format(col('fecha_proceso_date'), 'yyyy')
        ).withColumn(
            'mes_proceso',
            date_format(col('fecha_proceso_date'), 'MM')
        )
        
        logger.info("Columnas calculadas añadidas")
        return df
    
    def standardize_column_names(self, df: DataFrame) -> DataFrame:
        """
        Estandariza los nombres de columnas según convenciones de nomenclatura.
        
        Args:
            df: DataFrame con nombres originales
            
        Returns:
            DataFrame con nombres estandarizados
        """
        # Mapeo de nombres de columnas a estándar snake_case en español
        column_mapping = {
            'pais': 'pais_codigo',
            'fecha_proceso': 'fecha_proceso_original',
            'fecha_proceso_date': 'fecha_proceso',
            'transporte': 'codigo_transporte',
            'ruta': 'codigo_ruta',
            'tipo_entrega': 'tipo_entrega_codigo',
            'material': 'codigo_material',
            'precio': 'precio_unitario',
            'cantidad': 'cantidad_original',
            'unidad': 'unidad_original',
            'cantidad_normalizada': 'cantidad_unidades',
            'unidad_normalizada': 'unidad_estandar',
            'es_entrega_rutina': 'es_entrega_rutina',
            'es_entrega_bonificacion': 'es_entrega_bonificacion',
            'categoria_entrega': 'categoria_entrega',
            'valor_total': 'valor_total_calculado',
            'fecha_procesamiento': 'timestamp_procesamiento',
            'score_calidad_datos': 'score_calidad_datos',
            'id_registro': 'id_unico_registro',
            'año_proceso': 'anio_proceso',
            'mes_proceso': 'mes_proceso'
        }
        
        # Aplicar renombrado
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        logger.info("Nombres de columnas estandarizados")
        return df
    
    def generate_quality_metrics(self, df: DataFrame) -> Dict:
        """
        Genera métricas de calidad para el DataFrame procesado.
        
        Args:
            df: DataFrame procesado
            
        Returns:
            Dictionary con métricas de calidad
        """
        metrics = {
            'total_records': df.count(),
            'unique_countries': df.select('pais_codigo').distinct().count(),
            'unique_materials': df.select('codigo_material').distinct().count(),
            'date_range': {},
            'delivery_categories': {},
            'value_statistics': {},
            'quality_scores': {}
        }
        
        # Rango de fechas
        date_stats = df.agg(
            {'fecha_proceso': 'min', 'fecha_proceso': 'max'}
        ).collect()[0]
        metrics['date_range'] = {
            'min_date': str(date_stats[0]),
            'max_date': str(date_stats[1])
        }
        
        # Distribución por categorías de entrega
        category_dist = df.groupBy('categoria_entrega').count().collect()
        metrics['delivery_categories'] = {
            row['categoria_entrega']: row['count'] for row in category_dist
        }
        
        # Estadísticas de valor
        value_stats = df.agg(
            spark_sum('valor_total_calculado').alias('total_value'),
            mean('valor_total_calculado').alias('avg_value'),
            stddev('valor_total_calculado').alias('stddev_value')
        ).collect()[0]
        
        metrics['value_statistics'] = {
            'total_value': float(value_stats[0]) if value_stats[0] else 0,
            'average_value': float(value_stats[1]) if value_stats[1] else 0,
            'stddev_value': float(value_stats[2]) if value_stats[2] else 0
        }
        
        # Distribución de scores de calidad
        quality_dist = df.groupBy('score_calidad_datos').count().collect()
        metrics['quality_scores'] = {
            f'score_{row["score_calidad_datos"]}': row['count'] 
            for row in quality_dist
        }
        
        return metrics
    
    def process_data(self) -> DataFrame:
        """
        Ejecuta el pipeline completo de procesamiento de datos.
        
        Returns:
            DataFrame procesado y listo para escritura
        """
        logger.info("Iniciando procesamiento de datos...")
        
        # 1. Cargar datos
        df = self.load_data()
        
        # 2. Aplicar filtros
        df = self.apply_filters()
        
        # 3. Limpiar datos
        df = self.clean_data(df)
        
        # 4. Transformar unidades
        df = self.transform_units(df)
        
        # 5. Categorizar entregas
        df = self.categorize_deliveries(df)
        
        # 6. Añadir columnas calculadas
        df = self.add_calculated_columns(df)
        
        # 7. Estandarizar nombres de columnas
        df = self.standardize_column_names(df)
        
        # 8. Generar métricas de calidad final
        final_metrics = self.generate_quality_metrics(df)
        self.quality_report['final_metrics'] = final_metrics
        
        self.processed_df = df
        
        logger.info(f"Procesamiento completado. Registros finales: {df.count()}")
        return df
    
    def save_data(self, df: Optional[DataFrame] = None) -> None:
        """
        Guarda los datos procesados según la configuración.
        
        Args:
            df: DataFrame a guardar (opcional, usa processed_df por defecto)
        """
        if df is None:
            df = self.processed_df
        
        if df is None:
            raise ValueError("No hay datos para guardar. Ejecutar process_data() primero.")
        
        output_config = self.config.get('data.output')
        base_path = output_config['base_path']
        partition_by = output_config['partition_by']
        file_format = output_config['file_format']
        mode = output_config['mode']
        
        # Crear directorio de salida si no existe
        Path(base_path).mkdir(parents=True, exist_ok=True)
        
        # Guardar particionado por fecha_proceso
        try:
            if file_format.lower() == 'parquet':
                (df.write
                 .mode(mode)
                 .partitionBy(partition_by)
                 .parquet(base_path))
            elif file_format.lower() == 'csv':
                (df.write
                 .mode(mode)
                 .option("header", "true")
                 .partitionBy(partition_by)
                 .csv(base_path))
            else:
                raise ValueError(f"Formato no soportado: {file_format}")
            
            logger.info(f"Datos guardados en: {base_path} (formato: {file_format})")
            
        except Exception as e:
            logger.error(f"Error guardando datos: {e}")
            raise
    
    def save_quality_report(self, output_path: Optional[str] = None) -> None:
        """
        Guarda el reporte de calidad de datos.
        
        Args:
            output_path: Ruta personalizada para el reporte
        """
        if not output_path:
            output_config = self.config.get('data.output')
            output_path = f"{output_config['base_path']}/quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        import json
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.quality_report, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Reporte de calidad guardado en: {output_path}")
    
    def show_sample_data(self, df: Optional[DataFrame] = None, num_rows: int = 10) -> None:
        """
        Muestra una muestra de los datos procesados.
        
        Args:
            df: DataFrame a mostrar (opcional, usa processed_df por defecto)
            num_rows: Número de filas a mostrar
        """
        if df is None:
            df = self.processed_df
        
        if df is None:
            logger.warning("No hay datos para mostrar.")
            return
        
        print(f"\n=== MUESTRA DE DATOS ({num_rows} registros) ===")
        df.show(num_rows, truncate=False)
        
        print(f"\n=== SCHEMA ===")
        df.printSchema()
        
        print(f"\n=== ESTADÍSTICAS BÁSICAS ===")
        print(f"Total de registros: {df.count()}")
        print(f"Total de columnas: {len(df.columns)}")
    
    def cleanup(self) -> None:
        """Limpia recursos y cierra la sesión de Spark."""
        if self.spark:
            self.spark.stop()
            logger.info("Sesión de Spark cerrada")


# Funciones de utilidad
def create_data_processor_from_config(config_path: str, **overrides) -> DataProcessor:
    """
    Crea un DataProcessor desde un archivo de configuración.
    
    Args:
        config_path: Ruta al archivo de configuración
        **overrides: Parámetros de configuración a sobrescribir
        
    Returns:
        DataProcessor configurado
    """
    from config_manager import create_config_from_args
    
    config_manager = create_config_from_args(config_path, **overrides)
    return DataProcessor(config_manager)


if __name__ == "__main__":
    # Ejemplo de uso
    processor = create_data_processor_from_config(
        "config/base_config.yaml",
        start_date="2025-01-01",
        end_date="2025-06-30",
        country="GT"
    )
    
    try:
        # Procesar datos
        processed_df = processor.process_data()
        
        # Mostrar muestra
        processor.show_sample_data()
        
        # Guardar resultados
        processor.save_data()
        processor.save_quality_report()
        
    finally:
        processor.cleanup()