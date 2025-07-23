"""
Gestor de configuración usando OmegaConf para el proyecto ETL.
Maneja la carga y validación de configuraciones desde archivos YAML.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from omegaconf import DictConfig, OmegaConf
from pydantic import BaseModel, Field, validator
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataConfig(BaseModel):
    """Validación de configuración de datos."""
    
    class InputConfig(BaseModel):
        file_path: str
        file_format: str = "csv"
        header: bool = True
        delimiter: str = ","
        encoding: str = "utf-8"
    
    class OutputConfig(BaseModel):
        base_path: str
        partition_by: str = "fecha_proceso"
        file_format: str = "parquet"
        mode: str = "overwrite"
    
    class FiltersConfig(BaseModel):
        start_date: str
        end_date: str
        countries: Optional[List[str]] = None
        
        @validator('start_date', 'end_date')
        def validate_date_format(cls, v):
            from datetime import datetime
            try:
                datetime.strptime(v, '%Y-%m-%d')
                return v
            except ValueError:
                raise ValueError('Las fechas deben tener formato YYYY-MM-DD')
    
    input: InputConfig
    output: OutputConfig
    filters: FiltersConfig


class TransformationsConfig(BaseModel):
    """Validación de configuración de transformaciones."""
    
    class UnitConversionConfig(BaseModel):
        cs_to_units_multiplier: int = 20
        target_unit: str = "units"
    
    class DeliveryTypesConfig(BaseModel):
        routine: List[str] = ["ZPRE", "ZVE1"]
        bonus: List[str] = ["Z04", "Z05"]
        exclude: List[str] = ["COBR"]
    
    class DataCleaningConfig(BaseModel):
        remove_empty_materials: bool = True
        remove_zero_prices: bool = True
        remove_negative_quantities: bool = True
        fill_missing_countries: bool = False
    
    class AdditionalColumnsConfig(BaseModel):
        total_value: bool = True
        delivery_category: bool = True
        processing_date: bool = True
        data_quality_score: bool = True
    
    unit_conversion: UnitConversionConfig
    delivery_types: DeliveryTypesConfig
    data_cleaning: DataCleaningConfig
    additional_columns: AdditionalColumnsConfig


class SparkConfig(BaseModel):
    """Validación de configuración de Spark."""
    app_name: str = "ETL_Entregas_Productos"
    config: Dict[str, str] = {}


class ConfigManager:
    """
    Gestor principal de configuración usando OmegaConf.
    
    Proporciona métodos para cargar, validar y acceder a configuraciones
    desde archivos YAML con soporte para overrides de parámetros.
    """
    
    def __init__(self, config_path: Union[str, Path]):
        """
        Inicializa el gestor de configuración.
        
        Args:
            config_path: Ruta al archivo de configuración YAML principal
        """
        self.config_path = Path(config_path)
        self.config: Optional[DictConfig] = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Carga la configuración desde el archivo YAML."""
        try:
            if not self.config_path.exists():
                raise FileNotFoundError(f"Archivo de configuración no encontrado: {self.config_path}")
            
            self.config = OmegaConf.load(self.config_path)
            logger.info(f"Configuración cargada desde: {self.config_path}")
            
            # Validar configuración
            self._validate_config()
            
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            raise
    
    def _validate_config(self) -> None:
        """Valida la estructura de la configuración usando Pydantic."""
        try:
            # Validar secciones principales
            if 'data' in self.config:
                DataConfig(**self.config.data)
            
            if 'transformations' in self.config:
                TransformationsConfig(**self.config.transformations)
            
            if 'spark' in self.config:
                SparkConfig(**self.config.spark)
            
            logger.info("Configuración validada exitosamente")
            
        except Exception as e:
            logger.error(f"Error validando configuración: {e}")
            raise
    
    def override_config(self, **kwargs) -> None:
        """
        Sobrescribe parámetros de configuración.
        
        Args:
            **kwargs: Parámetros a sobrescribir usando notación de punto
                     (ej: data.filters.start_date="2025-01-01")
        """
        if not self.config:
            raise ValueError("Configuración no cargada")
        
        # Crear configuración de override
        override_dict = {}
        for key, value in kwargs.items():
            if value is not None:  # Solo override si el valor no es None
                OmegaConf.set(override_dict, key, value)
        
        if override_dict:
            override_config = OmegaConf.create(override_dict)
            self.config = OmegaConf.merge(self.config, override_config)
            logger.info(f"Configuración actualizada con overrides: {list(kwargs.keys())}")
            
            # Revalidar después del override
            self._validate_config()
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtiene un valor de configuración usando notación de punto.
        
        Args:
            key: Clave en notación de punto (ej: 'data.filters.start_date')
            default: Valor por defecto si la clave no existe
            
        Returns:
            Valor de configuración o default
        """
        if not self.config:
            raise ValueError("Configuración no cargada")
        
        return OmegaConf.select(self.config, key, default=default)
    
    def get_data_config(self) -> Dict[str, Any]:
        """Retorna la configuración de datos."""
        return OmegaConf.to_container(self.config.data, resolve=True)
    
    def get_transformations_config(self) -> Dict[str, Any]:
        """Retorna la configuración de transformaciones."""
        return OmegaConf.to_container(self.config.transformations, resolve=True)
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Retorna la configuración de Spark."""
        return OmegaConf.to_container(self.config.spark, resolve=True)
    
    def get_full_config(self) -> Dict[str, Any]:
        """Retorna la configuración completa como diccionario."""
        return OmegaConf.to_container(self.config, resolve=True)
    
    def save_config(self, output_path: Union[str, Path]) -> None:
        """
        Guarda la configuración actual en un archivo YAML.
        
        Args:
            output_path: Ruta donde guardar la configuración
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            OmegaConf.save(self.config, f)
        
        logger.info(f"Configuración guardada en: {output_path}")
    
    def print_config(self) -> None:
        """Imprime la configuración actual en formato YAML."""
        if self.config:
            print(OmegaConf.to_yaml(self.config))
        else:
            print("No hay configuración cargada")


def create_config_from_args(
    config_path: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    countries: Optional[List[str]] = None,
    input_path: Optional[str] = None,
    output_path: Optional[str] = None
) -> ConfigManager:
    """
    Crea un ConfigManager con overrides desde argumentos de línea de comandos.
    
    Args:
        config_path: Ruta al archivo de configuración base
        start_date: Fecha de inicio para filtrar
        end_date: Fecha de fin para filtrar
        country: País único para filtrar
        countries: Lista de países para filtrar
        input_path: Ruta personalizada del archivo de entrada
        output_path: Ruta personalizada de salida
        
    Returns:
        ConfigManager configurado
    """
    config_manager = ConfigManager(config_path)
    
    # Preparar overrides
    overrides = {}
    
    if start_date:
        overrides['data.filters.start_date'] = start_date
    
    if end_date:
        overrides['data.filters.end_date'] = end_date
    
    if country:
        overrides['data.filters.countries'] = [country]
    elif countries:
        overrides['data.filters.countries'] = countries
    
    if input_path:
        overrides['data.input.file_path'] = input_path
    
    if output_path:
        overrides['data.output.base_path'] = output_path
    
    # Aplicar overrides
    if overrides:
        config_manager.override_config(**overrides)
    
    return config_manager


if __name__ == "__main__":
    # Ejemplo de uso
    config_manager = ConfigManager("config/base_config.yaml")
    
    # Override de ejemplo
    config_manager.override_config(
        **{
            'data.filters.start_date': '2025-01-01',
            'data.filters.end_date': '2025-06-30',
            'data.filters.countries': ['GT', 'PE']
        }
    )
    
    # Acceso a configuración
    print("Fecha de inicio:", config_manager.get('data.filters.start_date'))
    print("Países:", config_manager.get('data.filters.countries'))
    
    # Imprimir configuración completa
    config_manager.print_config()