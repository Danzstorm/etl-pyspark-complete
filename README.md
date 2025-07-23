# ETL de Entregas de Productos con PySpark y OmegaConf

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4.0-orange)](https://spark.apache.org)
[![OmegaConf](https://img.shields.io/badge/OmegaConf-2.3.0-green)](https://omegaconf.readthedocs.io)

Sistema ETL robusto y parametrizable para el procesamiento de datos de entregas de productos, implementado con PySpark y configuración flexible usando OmegaConf.

## 🚀 Características Principales

- **Configuración Flexible**: Uso de OmegaConf con archivos YAML para configuraciones parametrizables
- **Procesamiento Escalable**: Implementado con PySpark para manejo eficiente de grandes volúmenes de datos
- **Filtrado Avanzado**: Filtros configurables por fecha, país y tipo de entrega
- **Calidad de Datos**: Detección y limpieza automática de anomalías
- **Transformaciones Inteligentes**: Normalización de unidades y categorización de entregas
- **Reportes de Calidad**: Generación automática de métricas y reportes de calidad
- **CLI Intuitiva**: Interfaz de línea de comandos con múltiples opciones

## 📋 Requisitos del Sistema

- Python 3.8 o superior
- Java 8 o superior (requerido por Spark)
- Mínimo 4GB de RAM
- 2GB de espacio libre en disco

## 🛠️ Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/Danzstorm/etl-pyspark-project.git
cd etl-pyspark-project
```

### 2. Crear Entorno Virtual

```bash
python -m venv venv

# En Linux/macOS
source venv/bin/activate

# En Windows
venv\Scripts\activate
```

### 3. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar Estructura del Proyecto

```bash
python -c "from src.utils import setup_project_directories; setup_project_directories()"
```

### 5. Preparar Datos

Coloca tu archivo CSV en `data/raw/entregas_productos_prueba.csv`

## 🎯 Uso Rápido

### Ejecución Básica

```bash
python main.py run
```

### Filtrar por Fechas y País

```bash
python main.py run --start-date 2025-01-01 --end-date 2025-06-30 --country GT
```

### Procesamiento para Múltiples Países

```bash
python main.py run --countries GT,PE,EC --start-date 2025-03-01 --end-date 2025-03-31
```

### Dry Run (Validación sin Guardar)

```bash
python main.py run --dry-run --verbose
```

## 📖 Comandos Disponibles

### Ejecutar Procesamiento

```bash
python main.py run [OPCIONES]
```

**Opciones principales:**
- `--config, -c`: Archivo de configuración (default: config/base_config.yaml)
- `--start-date, -s`: Fecha de inicio (YYYY-MM-DD)
- `--end-date, -e`: Fecha de fin (YYYY-MM-DD)
- `--country`: País específico (GT, PE, EC, SV, HN, JM)
- `--countries`: Lista de países separados por coma
- `--input-path`: Ruta personalizada del archivo de entrada
- `--output-path`: Directorio personalizado de salida
- `--dry-run`: Ejecutar sin guardar datos
- `--verbose, -v`: Logging detallado

### Validar Configuración

```bash
python main.py validate --config config/dev_config.yaml
```

### Crear Configuraciones de Ejemplo

```bash
python main.py create-configs --output-dir config
```

## 📁 Estructura del Proyecto

```
etl-pyspark-project/
├── README.md                 # Este archivo
├── requirements.txt          # Dependencias Python
├── main.py                  # Script principal CLI
├── config/                  # Configuraciones
│   ├── base_config.yaml     # Configuración base
│   ├── dev_config.yaml      # Configuración desarrollo
│   ├── qa_config.yaml       # Configuración QA
│   └── prod_config.yaml     # Configuración producción
├── src/                     # Código fuente
│   ├── config_manager.py    # Gestor de configuración
│   ├── data_processor.py    # Procesador principal
│   └── utils.py            # Utilidades
├── data/                   # Datos
│   ├── raw/                # Datos sin procesar
│   └── processed/          # Datos procesados (particionados)
├── logs/                   # Archivos de log
├── tests/                  # Pruebas unitarias
├── docs/                   # Documentación
└── notebooks/              # Jupyter notebooks de exploración
```

## ⚙️ Configuración

El proyecto usa OmegaConf para manejar configuraciones flexibles. La configuración base está en `config/base_config.yaml`.

### Configuraciones Principales

#### Filtros de Datos
```yaml
data:
  filters:
    start_date: "2025-01-01"
    end_date: "2025-12-31"
    countries: ["GT", "PE", "EC", "SV", "HN", "JM"]
```

#### Transformaciones
```yaml
transformations:
  unit_conversion:
    cs_to_units_multiplier: 20  # 1 CS = 20 unidades
  
  delivery_types:
    routine: ["ZPRE", "ZVE1"]     # Entregas de rutina
    bonus: ["Z04", "Z05"]         # Entregas con bonificaciones
    exclude: ["COBR"]             # Tipos excluidos
```

#### Limpieza de Datos
```yaml
transformations:
  data_cleaning:
    remove_empty_materials: true
    remove_zero_prices: true
    remove_negative_quantities: true
```

## 📊 Funcionalidades Implementadas

### ✅ Procesamiento de Datos

1. **Carga de Datos**: Lectura de archivos CSV con schema definido
2. **Filtrado Configurable**: Por fechas, países y tipos de entrega
3. **Particionado**: Por fecha_proceso para optimización
4. **Validación**: Schema y tipos de datos

### ✅ Transformaciones

1. **Normalización de Unidades**: CS (cajas) → unidades (CS * 20)
2. **Categorización de Entregas**: Rutina vs Bonificación
3. **Limpieza de Datos**: Eliminación de registros anómalos
4. **Columnas Calculadas**: Valor total, scores de calidad

### ✅ Calidad de Datos

1. **Detección de Anomalías**: Precios cero, cantidades negativas, materiales vacíos
2. **Reportes de Calidad**: Métricas detalladas de procesamiento
3. **Validación de Schema**: Tipos de datos y estructura
4. **Scores de Calidad**: Por registro individual

### ✅ Configuración y Parametrización

1. **OmegaConf**: Configuración flexible con YAML
2. **Overrides**: Parámetros desde línea de comandos
3. **Múltiples Entornos**: dev, qa, prod
4. **Validación de Configuración**: Automática con Pydantic

## 📈 Salidas del Procesamiento

### Datos Procesados
- **Formato**: Parquet (configurable a CSV)
- **Particionado**: Por fecha_proceso
- **Ubicación**: `data/processed/fecha_proceso=YYYYMMDD/`

### Columnas en el Dataset Final

| Columna | Descripción | Tipo |
|---------|-------------|------|
| `pais_codigo` | Código del país | string |
| `fecha_proceso` | Fecha de procesamiento | date |
| `codigo_transporte` | Código de transporte | string |
| `codigo_ruta` | Código de ruta | string |
| `tipo_entrega_codigo` | Código de tipo de entrega | string |
| `codigo_material` | Código del material | string |
| `precio_unitario` | Precio por unidad | double |
| `cantidad_unidades` | Cantidad normalizada en unidades | double |
| `unidad_estandar` | Unidad estándar (units) | string |
| `es_entrega_rutina` | Flag: es entrega de rutina | int |
| `es_entrega_bonificacion` | Flag: es entrega con bonificación | int |
| `categoria_entrega` | Categoría (RUTINA/BONIFICACION) | string |
| `valor_total_calculado` | Precio × cantidad normalizada | double |
| `score_calidad_datos` | Score de calidad (0-5) | int |
| `timestamp_procesamiento` | Timestamp de procesamiento | timestamp |
| `anio_proceso` | Año de proceso | string |
| `mes_proceso` | Mes de proceso | string |

### Reportes de Calidad
- **Formato**: JSON
- **Contenido**: Métricas de calidad, estadísticas, problemas detectados
- **Ubicación**: `data/processed/quality_report_YYYYMMDD_HHMMSS.json`

## 🔧 Desarrollo

### Estructura de Desarrollo en VS Code

1. **Instalar Extensiones Recomendadas**:
   - Python
   - Jupyter
   - YAML

2. **Configurar Python Interpreter**:
   ```
   Ctrl+Shift+P → Python: Select Interpreter → ./venv/bin/python
   ```

3. **Configurar Settings.json**:
   ```json
   {
       "python.defaultInterpreterPath": "./venv/bin/python",
       "python.linting.enabled": true,
       "python.linting.pylintEnabled": true
   }
   ```

### Desarrollo Local

```bash
# Ejecutar en modo desarrollo
python main.py run --config config/dev_config.yaml --verbose

# Validar configuración
python main.py validate --config config/dev_config.yaml

# Ejecutar tests
python -m pytest tests/ -v

# Formatear código
black src/ main.py

# Análisis estático
flake8 src/ main.py
```

## 🚀 Migración a Databricks

### 1. Preparar Archivos

```bash
# Crear zip con el código fuente
zip -r etl_project.zip src/ config/ main.py requirements.txt
```

### 2. Subir a Databricks

1. **Workspace** → **Import** → Subir `etl_project.zip`
2. **Clusters** → Crear cluster con Spark 3.4.0
3. **Libraries** → Instalar desde PyPI: omegaconf, pydantic

### 3. Adaptar Rutas

```python
# En Databricks, cambiar rutas locales por DBFS
input_path = "/dbfs/FileStore/data/entregas_productos_prueba.csv"
output_path = "/dbfs/FileStore/processed/"
```

### 4. Ejecutar en Notebook

```python
%run ./main.py run --config /dbfs/FileStore/config/base_config.yaml
```

## 🧪 Pruebas

```bash
# Ejecutar todas las pruebas
python -m pytest tests/ -v

# Ejecutar con cobertura
python -m pytest tests/ --cov=src --cov-report=html

# Prueba específica
python -m pytest tests/test_data_processor.py::test_load_data -v
```

## 📚 Documentación Adicional

- [Flujo de Datos Detallado](docs/flujo_datos.md)
- [Configuración Avanzada](docs/configuracion.md)
- [Guía de Troubleshooting](docs/troubleshooting.md)
- [API Reference](docs/api_reference.md)

## 🤝 Contribución

1. Fork el repositorio
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver [LICENSE](LICENSE) para más detalles.

## 🆘 Soporte

Para reportar bugs o solicitar nuevas funcionalidades, crear un [issue](https://github.com/tu-usuario/etl-pyspark-project/issues).

## 🏆 Características Destacadas

- ✅ **Configuración Flexible**: OmegaConf con YAML
- ✅ **Escalabilidad**: PySpark para big data
- ✅ **Calidad de Datos**: Validación y limpieza automática
- ✅ **Parametrización**: Filtros configurables
- ✅ **Monitoreo**: Reportes de calidad detallados
- ✅ **CLI Amigable**: Interfaz intuitiva
- ✅ **Documentación Completa**: Guías y ejemplos
- ✅ **Estándares de Código**: PEP8, type hints, docstrings
- ✅ **Testing**: Pruebas unitarias
- ✅ **Compatibilidad**: Local, Databricks, otros clusters

---
