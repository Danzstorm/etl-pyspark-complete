"""
Pruebas unitarias para el procesador de datos.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import shutil

# Añadir src al path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.config_manager import ConfigManager
from src.data_processor import DataProcessor


class TestDataProcessor:
    """Clase de pruebas para DataProcessor."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture de SparkSession para las pruebas."""
        spark = SparkSession.builder \
            .appName("test_etl") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_config(self):
        """Fixture con configuración de prueba."""
        config_data = {
            'app': {
                'name': 'test_etl',
                'version': '1.0.0',
                'environment': 'test'
            },
            'data': {
                'input': {
                    'file_path': 'test_data.csv',
                    'file_format': 'csv',
                    'header': True,
                    'delimiter': ',',
                    'encoding': 'utf-8'
                },
                'output': {
                    'base_path': 'test_output',
                    'partition_by': 'fecha_proceso',
                    'file_format': 'parquet',
                    'mode': 'overwrite'
                },
                'filters': {
                    'start_date': '2025-01-01',
                    'end_date': '2025-12-31',
                    'countries': ['GT', 'PE']
                }
            },
            'transformations': {
                'unit_conversion': {
                    'cs_to_units_multiplier': 20,
                    'target_unit': 'units'
                },
                'delivery_types': {
                    'routine': ['ZPRE', 'ZVE1'],
                    'bonus': ['Z04', 'Z05'],
                    'exclude': ['COBR']
                },
                'data_cleaning': {
                    'remove_empty_materials': True,
                    'remove_zero_prices': True,
                    'remove_negative_quantities': True,
                    'fill_missing_countries': False
                },
                'additional_columns': {
                    'total_value': True,
                    'delivery_category': True,
                    'processing_date': True,
                    'data_quality_score': True
                }
            },
            'spark': {
                'app_name': 'test_etl',
                'config': {
                    'spark.sql.adaptive.enabled': 'true'
                }
            }
        }
        
        # Crear archivo temporal de configuración
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            config_path = f.name
        
        config_manager = ConfigManager(config_path)
        
        yield config_manager
        
        # Limpiar archivo temporal
        Path(config_path).unlink()
    
    @pytest.fixture
    def sample_data(self, spark):
        """Fixture con datos de prueba."""
        schema = StructType([
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
        
        data = [
            ("GT", "20250513", "67053596", "919885", "ZPRE", "AA004003", 3195.54, 100.0, "CS"),
            ("GT", "20250513", "67053596", "919885", "ZPRE", "BA018426", 529.99, 20.0, "CS"),
            ("PE", "20250114", "67050264", "5428813", "ZPRE", "BA003708", 260.0, 10.0, "CS"),
            ("PE", "20250114", "67050264", "5428813", "ZPRE", "BA003708", 32.5, 5.0, "ST"),
            ("EC", "20250217", "67051860", "4511092", "COBR", "", 1812.44, 1.0, "ST"),  # Datos problemáticos
            ("SV", "20250325", "67052854", "2161885", "ZPRE", "AA015008", 0.0, 2.0, "ST"),  # Precio cero
            ("HN", "20250314", "67052658", "2710948", "Z04", "AA001001", 2000.0, -10.0, "CS"),  # Cantidad negativa
        ]
        
        return spark.createDataFrame(data, schema)
    
    def test_init_spark(self, sample_config):
        """Prueba la inicialización de Spark."""
        processor = DataProcessor(sample_config)
        assert processor.spark is not None
        assert processor.spark.sparkContext.appName == "test_etl"
    
    def test_define_schema(self, sample_config):
        """Prueba la definición del schema."""
        processor = DataProcessor(sample_config)
        schema = processor._define_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 9
        assert schema.fieldNames() == [
            'pais', 'fecha_proceso', 'transporte', 'ruta',
            'tipo_entrega', 'material', 'precio', 'cantidad', 'unidad'
        ]
    
    def test_apply_filters(self, sample_config, sample_data):
        """Prueba la aplicación de filtros."""
        processor = DataProcessor(sample_config)
        processor.input_df = sample_data
        
        # Mock del comportamiento de fechas
        filtered_df = processor.apply_filters()
        
        # Verificar que se aplicaron los filtros
        countries = [row['pais'] for row in filtered_df.select('pais').distinct().collect()]
        assert all(country in ['GT', 'PE'] for country in countries)
    
    def test_clean_data(self, sample_config, sample_data):
        """Prueba la limpieza de datos."""
        processor = DataProcessor(sample_config)
        
        # Aplicar limpieza
        cleaned_df = processor.clean_data(sample_data)
        
        # Verificar que se eliminaron registros problemáticos
        materials = [row['material'] for row in cleaned_df.select('material').collect()]
        prices = [row['precio'] for row in cleaned_df.select('precio').collect()]
        quantities = [row['cantidad'] for row in cleaned_df.select('cantidad').collect()]
        
        # No debe haber materiales vacíos
        assert "" not in materials
        assert None not in materials
        
        # No debe haber precios cero o negativos
        assert all(price > 0 for price in prices)
        
        # No debe haber cantidades negativas
        assert all(quantity > 0 for quantity in quantities)
    
    def test_transform_units(self, sample_config, sample_data):
        """Prueba la transformación de unidades."""
        processor = DataProcessor(sample_config)
        
        transformed_df = processor.transform_units(sample_data)
        
        # Verificar que las unidades CS se transformaron correctamente
        cs_rows = transformed_df.filter(transformed_df.unidad == "CS").collect()
        
        for row in cs_rows:
            expected_quantity = row['cantidad'] * 20  # CS multiplier
            assert row['cantidad_normalizada'] == expected_quantity
            assert row['unidad_normalizada'] == 'units'
    
    def test_categorize_deliveries(self, sample_config, sample_data):
        """Prueba la categorización de entregas."""
        processor = DataProcessor(sample_config)
        
        categorized_df = processor.categorize_deliveries(sample_data)
        
        # Verificar que se crearon las columnas de categorización
        assert 'es_entrega_rutina' in categorized_df.columns
        assert 'es_entrega_bonificacion' in categorized_df.columns
        assert 'categoria_entrega' in categorized_df.columns
        
        # Verificar categorización correcta
        zpre_rows = categorized_df.filter(categorized_df.tipo_entrega == "ZPRE").collect()
        for row in zpre_rows:
            assert row['es_entrega_rutina'] == 1
            assert row['categoria_entrega'] == 'RUTINA'
        
        z04_rows = categorized_df.filter(categorized_df.tipo_entrega == "Z04").collect()
        for row in z04_rows:
            assert row['es_entrega_bonificacion'] == 1
            assert row['categoria_entrega'] == 'BONIFICACION'
    
    def test_add_calculated_columns(self, sample_config, sample_data):
        """Prueba la adición de columnas calculadas."""
        processor = DataProcessor(sample_config)
        
        # Primero transformar unidades para tener cantidad_normalizada
        df_with_units = processor.transform_units(sample_data)
        df_with_calculated = processor.add_calculated_columns(df_with_units)
        
        # Verificar que se agregaron las columnas calculadas
        expected_columns = [
            'valor_total', 'fecha_procesamiento', 'score_calidad_datos',
            'id_registro', 'año_proceso', 'mes_proceso'
        ]
        
        for col in expected_columns:
            assert col in df_with_calculated.columns
        
        # Verificar cálculo de valor total
        rows = df_with_calculated.collect()
        for row in rows:
            expected_total = row['precio'] * row['cantidad_normalizada']
            assert abs(row['valor_total'] - expected_total) < 0.01  # Tolerancia para float
    
    def test_standardize_column_names(self, sample_config, sample_data):
        """Prueba la estandarización de nombres de columnas."""
        processor = DataProcessor(sample_config)
        
        # Agregar algunas columnas que se renombrarán
        df_with_extra_cols = sample_data.withColumn('cantidad_normalizada', sample_data.cantidad)
        standardized_df = processor.standardize_column_names(df_with_extra_cols)
        
        # Verificar que se aplicaron los renombrados
        assert 'pais_codigo' in standardized_df.columns
        assert 'precio_unitario' in standardized_df.columns
        assert 'cantidad_unidades' in standardized_df.columns
        
        # Verificar que las columnas originales fueron renombradas
        assert 'pais' not in standardized_df.columns
        assert 'precio' not in standardized_df.columns
    
    def test_generate_quality_metrics(self, sample_config, sample_data):
        """Prueba la generación de métricas de calidad."""
        processor = DataProcessor(sample_config)
        
        # Preparar DataFrame con columnas necesarias
        df = processor.transform_units(sample_data)
        df = processor.categorize_deliveries(df)
        df = processor.add_calculated_columns(df)
        df = processor.standardize_column_names(df)
        
        metrics = processor.generate_quality_metrics(df)
        
        # Verificar estructura de métricas
        assert 'total_records' in metrics
        assert 'unique_countries' in metrics
        assert 'unique_materials' in metrics
        assert 'delivery_categories' in metrics
        assert 'value_statistics' in metrics
        
        # Verificar valores básicos
        assert metrics['total_records'] > 0
        assert metrics['unique_countries'] > 0
    
    @patch('pathlib.Path.exists')
    def test_load_data_file_not_found(self, mock_exists, sample_config):
        """Prueba el manejo de archivos no encontrados."""
        mock_exists.return_value = False
        
        processor = DataProcessor(sample_config)
        
        with pytest.raises(FileNotFoundError):
            processor.load_data()
    
    def test_process_data_integration(self, sample_config):
        """Prueba de integración del pipeline completo."""
        processor = DataProcessor(sample_config)
        
        # Crear datos de prueba en memoria
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad\n")
            f.write("GT,20250513,67053596,919885,ZPRE,AA004003,3195.54,100.0,CS\n")
            f.write("PE,20250114,67050264,5428813,ZPRE,BA003708,260.0,10.0,CS\n")
            csv_path = f.name
        
        # Actualizar configuración para usar archivo temporal
        processor.config.override_config(**{'data.input.file_path': csv_path})
        
        try:
            # Ejecutar pipeline completo
            result_df = processor.process_data()
            
            # Verificar que el resultado no está vacío
            assert result_df.count() > 0
            
            # Verificar que se aplicaron las transformaciones
            columns = result_df.columns
            assert 'pais_codigo' in columns
            assert 'cantidad_unidades' in columns
            assert 'categoria_entrega' in columns
            assert 'valor_total_calculado' in columns
            
        finally:
            # Limpiar archivo temporal
            Path(csv_path).unlink()
    
    def test_save_data(self, sample_config, sample_data):
        """Prueba el guardado de datos."""
        processor = DataProcessor(sample_config)
        processor.processed_df = sample_data
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Actualizar configuración para usar directorio temporal
            processor.config.override_config(**{'data.output.base_path': temp_dir})
            
            # Guardar datos
            processor.save_data()
            
            # Verificar que se crearon archivos
            output_path = Path(temp_dir)
            assert output_path.exists()
            # En Parquet particionado, se crean subdirectorios
            assert any(output_path.iterdir())
    
    def test_quality_report_generation(self, sample_config, sample_data):
        """Prueba la generación del reporte de calidad."""
        processor = DataProcessor(sample_config)
        processor.input_df = sample_data
        processor._generate_initial_quality_report()
        
        report = processor.quality_report
        
        # Verificar estructura del reporte
        assert 'initial_records' in report
        assert 'columns_with_nulls' in report
        assert 'empty_materials' in report
        assert 'zero_prices' in report
        assert 'negative_quantities' in report
        
        # Verificar detección de problemas
        assert report['initial_records'] == sample_data.count()
        assert report['empty_materials'] > 0  # Hay un material vacío en los datos de prueba
        assert report['zero_prices'] > 0      # Hay un precio cero en los datos de prueba


@pytest.mark.integration
class TestDataProcessorIntegration:
    """Pruebas de integración más complejas."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture de SparkSession para las pruebas."""
        spark = SparkSession.builder \
            .appName("test_etl_integration") \
            .master("local[2]") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()
    
    def test_end_to_end_processing(self, spark):
        """Prueba end-to-end del procesamiento completo."""
        # Crear configuración temporal
        config_data = {
            'data': {
                'input': {'file_path': 'test.csv'},
                'output': {'base_path': 'test_output'},
                'filters': {
                    'start_date': '2025-01-01',
                    'end_date': '2025-12-31',
                    'countries': None
                }
            },
            'transformations': {
                'unit_conversion': {'cs_to_units_multiplier': 20},
                'delivery_types': {
                    'routine': ['ZPRE'],
                    'bonus': ['Z04'],
                    'exclude': ['COBR']
                },
                'data_cleaning': {
                    'remove_empty_materials': True,
                    'remove_zero_prices': True,
                    'remove_negative_quantities': True
                },
                'additional_columns': {
                    'total_value': True,
                    'delivery_category': True,
                    'processing_date': True,
                    'data_quality_score': True
                }
            },
            'spark': {'app_name': 'test_integration'}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            config_path = f.name
        
        # Crear archivo CSV de prueba
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad\n")
            f.write("GT,20250513,67053596,919885,ZPRE,AA004003,3195.54,100.0,CS\n")
            f.write("PE,20250114,67050264,5428813,ZPRE,BA003708,260.0,10.0,ST\n")
            f.write("EC,20250217,67051860,4511092,Z04,BA017072,156.6,30.0,CS\n")
            csv_path = f.name
        
        try:
            # Crear y configurar procesador
            config_manager = ConfigManager(config_path)
            config_manager.override_config(**{'data.input.file_path': csv_path})
            
            processor = DataProcessor(config_manager)
            
            # Ejecutar procesamiento completo
            result_df = processor.process_data()
            
            # Verificaciones
            assert result_df.count() == 3  # Todos los registros son válidos
            
            # Verificar transformaciones aplicadas
            assert 'cantidad_unidades' in result_df.columns
            assert 'categoria_entrega' in result_df.columns
            assert 'valor_total_calculado' in result_df.columns
            
            # Verificar cálculos
            cs_row = result_df.filter(result_df.unidad_original == "CS").first()
            assert cs_row['cantidad_unidades'] == cs_row['cantidad_original'] * 20
            
            # Verificar categorización
            zpre_rows = result_df.filter(result_df.tipo_entrega_codigo == "ZPRE").count()
            z04_rows = result_df.filter(result_df.tipo_entrega_codigo == "Z04").count()
            assert zpre_rows == 2
            assert z04_rows == 1
            
        finally:
            # Limpiar archivos temporales
            Path(config_path).unlink()
            Path(csv_path).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])