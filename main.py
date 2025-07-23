#!/usr/bin/env python3
"""
Script principal para el ETL de entregas de productos.
Proporciona interfaz de línea de comandos para ejecutar el procesamiento de datos.
"""

import click
import sys
from pathlib import Path
from datetime import datetime
import logging

# Añadir src al path para importaciones
sys.path.append(str(Path(__file__).parent / "src"))

from src.config_manager import create_config_from_args
from src.data_processor import DataProcessor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/etl_process.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--config', '-c',
    default='config/base_config.yaml',
    help='Ruta al archivo de configuración YAML',
    type=click.Path(exists=True)
)
@click.option(
    '--start-date', '-s',
    help='Fecha de inicio para filtrar (formato: YYYY-MM-DD)',
    type=click.DateTime(formats=['%Y-%m-%d'])
)
@click.option(
    '--end-date', '-e',
    help='Fecha de fin para filtrar (formato: YYYY-MM-DD)',
    type=click.DateTime(formats=['%Y-%m-%d'])
)
@click.option(
    '--country',
    help='País específico para filtrar (ej: GT, PE, EC)',
    type=str
)
@click.option(
    '--countries',
    help='Lista de países separados por coma (ej: GT,PE,EC)',
    type=str
)
@click.option(
    '--input-path',
    help='Ruta personalizada del archivo de entrada',
    type=click.Path()
)
@click.option(
    '--output-path',
    help='Ruta personalizada de salida',
    type=click.Path()
)
@click.option(
    '--show-sample/--no-show-sample',
    default=True,
    help='Mostrar muestra de datos procesados'
)
@click.option(
    '--save-quality-report/--no-save-quality-report',
    default=True,
    help='Guardar reporte de calidad de datos'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Ejecutar sin guardar datos (solo procesamiento y validación)'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Habilitar logging detallado'
)
def main(
    config: str,
    start_date: datetime,
    end_date: datetime,
    country: str,
    countries: str,
    input_path: str,
    output_path: str,
    show_sample: bool,
    save_quality_report: bool,
    dry_run: bool,
    verbose: bool
):
    """
    ETL para procesamiento de entregas de productos.
    
    Procesa datos de entregas aplicando filtros configurables,
    transformaciones de unidades, categorización de entregas
    y generación de reportes de calidad.
    
    Ejemplos de uso:
    
    \b
    # Procesamiento básico con configuración por defecto
    python main.py
    
    \b
    # Filtrar por rango de fechas y país específico
    python main.py --start-date 2025-01-01 --end-date 2025-06-30 --country GT
    
    \b
    # Procesamiento para múltiples países
    python main.py --countries GT,PE,EC --start-date 2025-03-01 --end-date 2025-03-31
    
    \b
    # Dry run (no guardar datos)
    python main.py --dry-run --verbose
    """
    
    # Configurar nivel de logging
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Logging detallado habilitado")
    
    # Crear directorio de logs si no existe
    Path('logs').mkdir(exist_ok=True)
    
    try:
        logger.info("=== INICIANDO ETL DE ENTREGAS DE PRODUCTOS ===")
        
        # Procesar argumentos
        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
        countries_list = countries.split(',') if countries else None
        
        # Mostrar parámetros de ejecución
        logger.info("Parámetros de ejecución:")
        logger.info(f"  - Configuración: {config}")
        logger.info(f"  - Fecha inicio: {start_date_str}")
        logger.info(f"  - Fecha fin: {end_date_str}")
        logger.info(f"  - País: {country}")
        logger.info(f"  - Países: {countries_list}")
        logger.info(f"  - Archivo entrada: {input_path}")
        logger.info(f"  - Directorio salida: {output_path}")
        logger.info(f"  - Dry run: {dry_run}")
        
        # Crear configuración con overrides
        config_manager = create_config_from_args(
            config_path=config,
            start_date=start_date_str,
            end_date=end_date_str,
            country=country,
            countries=countries_list,
            input_path=input_path,
            output_path=output_path
        )
        
        if verbose:
            logger.debug("Configuración final:")
            config_manager.print_config()
        
        # Crear procesador de datos
        processor = DataProcessor(config_manager)
        
        logger.info("=== INICIANDO PROCESAMIENTO DE DATOS ===")
        
        # Procesar datos
        processed_df = processor.process_data()
        
        # Mostrar estadísticas del procesamiento
        logger.info("=== ESTADÍSTICAS DE PROCESAMIENTO ===")
        initial_records = processor.quality_report.get('initial_records', 0)
        final_records = processed_df.count()
        
        logger.info(f"Registros iniciales: {initial_records:,}")
        logger.info(f"Registros finales: {final_records:,}")
        logger.info(f"Registros filtrados/eliminados: {initial_records - final_records:,}")
        
        if final_records == 0:
            logger.warning("⚠️  No hay datos para procesar después de aplicar filtros")
            return
        
        # Mostrar muestra de datos si se solicita
        if show_sample:
            processor.show_sample_data(num_rows=5)
        
        # Mostrar métricas de calidad
        final_metrics = processor.quality_report.get('final_metrics', {})
        if final_metrics:
            logger.info("=== MÉTRICAS DE CALIDAD ===")
            logger.info(f"Países únicos: {final_metrics.get('unique_countries', 0)}")
            logger.info(f"Materiales únicos: {final_metrics.get('unique_materials', 0)}")
            
            date_range = final_metrics.get('date_range', {})
            if date_range:
                logger.info(f"Rango de fechas: {date_range.get('min_date')} a {date_range.get('max_date')}")
            
            delivery_cats = final_metrics.get('delivery_categories', {})
            if delivery_cats:
                logger.info("Distribución por categoría de entrega:")
                for category, count in delivery_cats.items():
                    logger.info(f"  - {category}: {count:,} registros")
            
            value_stats = final_metrics.get('value_statistics', {})
            if value_stats:
                total_value = value_stats.get('total_value', 0)
                avg_value = value_stats.get('average_value', 0)
                logger.info(f"Valor total procesado: ${total_value:,.2f}")
                logger.info(f"Valor promedio por registro: ${avg_value:.2f}")
        
        # Guardar datos y reportes (a menos que sea dry run)
        if not dry_run:
            logger.info("=== GUARDANDO RESULTADOS ===")
            
            # Guardar datos procesados
            processor.save_data()
            
            # Guardar reporte de calidad
            if save_quality_report:
                processor.save_quality_report()
            
            logger.info("✅ Procesamiento completado exitosamente")
        else:
            logger.info("✅ Dry run completado (datos no guardados)")
        
        # Mostrar ubicación de archivos de salida
        if not dry_run:
            output_config = config_manager.get('data.output')
            output_base_path = output_config['base_path']
            logger.info(f"📁 Datos guardados en: {Path(output_base_path).absolute()}")
        
    except KeyboardInterrupt:
        logger.warning("⚠️  Procesamiento interrumpido por el usuario")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Error durante el procesamiento: {e}")
        if verbose:
            import traceback
            logger.error(f"Traceback completo:\n{traceback.format_exc()}")
        sys.exit(1)
        
    finally:
        # Limpiar recursos
        try:
            if 'processor' in locals():
                processor.cleanup()
        except Exception as e:
            logger.warning(f"Error limpiando recursos: {e}")
        
        logger.info("=== ETL FINALIZADO ===")


@click.command()
@click.option(
    '--config', '-c',
    default='config/base_config.yaml',
    help='Ruta al archivo de configuración YAML',
    type=click.Path(exists=True)
)
def validate_config(config: str):
    """Valida un archivo de configuración sin ejecutar el procesamiento."""
    try:
        from src.config_manager import ConfigManager
        
        logger.info(f"Validando configuración: {config}")
        config_manager = ConfigManager(config)
        
        logger.info("✅ Configuración válida")
        
        # Mostrar resumen de la configuración
        print("\n=== RESUMEN DE CONFIGURACIÓN ===")
        config_manager.print_config()
        
    except Exception as e:
        logger.error(f"❌ Error en la configuración: {e}")
        sys.exit(1)


@click.command()
@click.option(
    '--output-dir',
    default='config',
    help='Directorio donde crear las configuraciones de ejemplo'
)
def create_sample_configs(output_dir: str):
    """Crea archivos de configuración de ejemplo para diferentes entornos."""
    try:
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Configuraciones de ejemplo para diferentes entornos
        configs = {
            'dev_config.yaml': {
                'app': {'environment': 'development'},
                'data': {
                    'filters': {
                        'start_date': '2025-01-01',
                        'end_date': '2025-03-31',
                        'countries': ['GT', 'PE']
                    }
                },
                'logging': {'level': 'DEBUG'}
            },
            'qa_config.yaml': {
                'app': {'environment': 'qa'},
                'data': {
                    'filters': {
                        'start_date': '2025-01-01',
                        'end_date': '2025-06-30'
                    }
                },
                'logging': {'level': 'INFO'}
            },
            'prod_config.yaml': {
                'app': {'environment': 'production'},
                'data': {
                    'output': {
                        'base_path': '/data/processed/entregas_productos',
                        'file_format': 'parquet'
                    }
                },
                'logging': {'level': 'INFO'}
            }
        }
        
        from omegaconf import OmegaConf
        
        for filename, config_override in configs.items():
            file_path = output_path / filename
            
            # Cargar configuración base y aplicar overrides
            base_config = OmegaConf.load('config/base_config.yaml')
            override_config = OmegaConf.create(config_override)
            merged_config = OmegaConf.merge(base_config, override_config)
            
            # Guardar configuración
            OmegaConf.save(merged_config, file_path)
            logger.info(f"Configuración creada: {file_path}")
        
        logger.info("✅ Configuraciones de ejemplo creadas")
        
    except Exception as e:
        logger.error(f"❌ Error creando configuraciones: {e}")
        sys.exit(1)


# Grupo de comandos CLI
@click.group()
def cli():
    """ETL para procesamiento de entregas de productos usando PySpark y OmegaConf."""
    pass


# Registrar comandos
cli.add_command(main, name='run')
cli.add_command(validate_config, name='validate')
cli.add_command(create_sample_configs, name='create-configs')


if __name__ == '__main__':
    cli()