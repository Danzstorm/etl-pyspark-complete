#!/bin/bash

# Script de configuración inicial para el proyecto ETL
# Ejecutar: bash setup_project.sh

set -e  # Salir si cualquier comando falla

echo "🚀 Configurando proyecto ETL de Entregas de Productos..."

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para mostrar mensajes
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar Python
print_status "Verificando Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_success "Python encontrado: $PYTHON_VERSION"
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version | cut -d' ' -f2)
    print_success "Python encontrado: $PYTHON_VERSION"
    PYTHON_CMD="python"
else
    print_error "Python no encontrado. Por favor instala Python 3.8+"
    exit 1
fi

# Verificar Java (requerido para Spark)
print_status "Verificando Java..."
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    print_success "Java encontrado: $JAVA_VERSION"
else
    print_warning "Java no encontrado. Spark requiere Java 8+"
    print_status "Instalando OpenJDK..."
    
    # Detectar sistema operativo e instalar Java
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get update && sudo apt-get install -y openjdk-8-jdk
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v brew &> /dev/null; then
            brew install openjdk@8
        else
            print_error "Homebrew no encontrado. Instala Java manualmente."
            exit 1
        fi
    else
        print_warning "Sistema operativo no reconocido. Instala Java manualmente."
    fi
fi

# Crear entorno virtual
print_status "Creando entorno virtual..."
if [ ! -d "venv" ]; then
    $PYTHON_CMD -m venv venv
    print_success "Entorno virtual creado"
else
    print_warning "Entorno virtual ya existe"
fi

# Activar entorno virtual
print_status "Activando entorno virtual..."
source venv/bin/activate

# Actualizar pip
print_status "Actualizando pip..."
pip install --upgrade pip

# Instalar dependencias
print_status "Instalando dependencias..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    print_success "Dependencias instaladas"
else
    print_error "requirements.txt no encontrado"
    exit 1
fi

# Crear estructura de directorios
print_status "Creando estructura de directorios..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p logs
mkdir -p tests
mkdir -p docs
mkdir -p notebooks

# Crear archivos __init__.py
touch src/__init__.py
touch tests/__init__.py

print_success "Estructura de directorios creada"

# Verificar instalación de PySpark
print_status "Verificando instalación de PySpark..."
if $PYTHON_CMD -c "import pyspark; print('PySpark version:', pyspark.__version__)" 2>/dev/null; then
    print_success "PySpark instalado correctamente"
else
    print_error "Error en la instalación de PySpark"
    exit 1
fi

# Crear configuraciones de ejemplo
print_status "Creando configuraciones de ejemplo..."
$PYTHON_CMD -c "
import sys
sys.path.append('src')
try:
    from main import create_sample_configs
    create_sample_configs.callback('config')
    print('Configuraciones de ejemplo creadas')
except Exception as e:
    print(f'Error creando configuraciones: {e}')
"

# Verificar archivo de datos
print_status "Verificando archivo de datos..."
if [ -f "data/raw/entregas_productos_prueba.csv" ]; then
    print_success "Archivo de datos encontrado"
else
    print_warning "Archivo de datos no encontrado en data/raw/"
    print_status "Creando archivo de ejemplo..."
    
    # Crear archivo CSV de ejemplo
    cat > data/raw/entregas_productos_prueba.csv << 'EOF'
pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad
GT,20250513,67053596,919885,ZPRE,AA004003,3195.540000000000000000,100.000000000000000000,CS
GT,20250513,67053596,919885,ZPRE,BA018426,529.990000000000000000,20.000000000000000000,CS
PE,20250114,67050264,5428813,ZPRE,BA003708,260.000000000000000000,10.000000000000000000,CS
PE,20250114,67050264,5428813,ZPRE,BA003708,32.500000000000000000,5.000000000000000000,ST
EC,20250217,67051860,4511092,Z04,BA017072,156.600000000000000000,30.000000000000000000,CS
SV,20250325,67052854,2161885,ZPRE,AA015008,1421.420000000000000000,2.000000000000000000,ST
HN,20250314,67052658,2710948,Z05,AA001001,2000.000000000000000000,10.000000000000000000,CS
JM,20250602,67053946,8200894,ZVE1,AA531705,124200.000000000000000000,30.000000000000000000,CS
EOF
    
    print_success "Archivo de ejemplo creado"
fi

# Test básico del sistema
print_status "Ejecutando test básico..."
if $PYTHON_CMD main.py validate --config config/base_config.yaml; then
    print_success "Configuración válida"
else
    print_error "Error en la configuración"
    exit 1
fi

# Ejecutar dry run
print_status "Ejecutando dry run del ETL..."
if $PYTHON_CMD main.py run --dry-run --verbose; then
    print_success "Dry run completado exitosamente"
else
    print_warning "Dry run con errores - revisar configuración"
fi

# Crear script de activación
print_status "Creando script de activación..."
cat > activate_env.sh << 'EOF'
#!/bin/bash
# Script para activar el entorno del proyecto
source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
echo "🚀 Entorno ETL activado"
echo "Uso: python main.py run --help"
EOF

chmod +x activate_env.sh
print_success "Script de activación creado: ./activate_env.sh"

# Crear script para VS Code
print_status "Configurando VS Code..."
mkdir -p .vscode

cat > .vscode/settings.json << 'EOF'
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests"],
    "files.associations": {
        "*.yaml": "yaml",
        "*.yml": "yaml"
    },
    "yaml.schemas": {
        "config/base_config.yaml": "config/*.yaml"
    }
}
EOF

cat > .vscode/launch.json << 'EOF'
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "ETL Run",
            "type": "python",
            "request": "launch",
            "program": "main.py",
            "args": ["run", "--dry-run", "--verbose"],
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}",
            "env": {
                "PYTHONPATH": "${workspaceFolder}/src"
            }
        },
        {
            "name": "ETL Test",
            "type": "python",
            "request": "launch",
            "program": "main.py",
            "args": ["run", "--config", "config/dev_config.yaml", "--country", "GT"],
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}",
            "env": {
                "PYTHONPATH": "${workspaceFolder}/src"
            }
        }
    ]
}
EOF

print_success "Configuración de VS Code creada"

# Resumen final
echo ""
echo "🎉 ¡Configuración completada exitosamente!"
echo ""
echo "📁 Estructura del proyecto:"
echo "   ├── src/              # Código fuente"
echo "   ├── config/           # Configuraciones YAML"
echo "   ├── data/raw/         # Datos de entrada"
echo "   ├── data/processed/   # Datos procesados"
echo "   ├── logs/             # Archivos de log"
echo "   ├── tests/            # Pruebas unitarias"
echo "   └── venv/             # Entorno virtual"
echo ""
echo "🚀 Próximos pasos:"
echo "   1. Activar entorno: source ./activate_env.sh"
echo "   2. Colocar datos CSV en: data/raw/entregas_productos_prueba.csv"
echo "   3. Ejecutar ETL: python main.py run"
echo "   4. Ver resultados en: data/processed/"
echo ""
echo "📖 Comandos útiles:"
echo "   python main.py run --help                    # Ver opciones"
echo "   python main.py run --dry-run                 # Test sin guardar"
echo "   python main.py run --country GT              # Filtrar por país"
echo "   python main.py validate                      # Validar config"
echo "   python -m pytest tests/ -v                   # Ejecutar tests"
echo ""
echo "🔗 Para Databricks:"
echo "   1. Crear cluster con Spark 3.4.0+"
echo "   2. Instalar: omegaconf, pydantic"
echo "   3. Subir archivos del proyecto"
echo "   4. Adaptar rutas a DBFS"
echo ""
print_success "¡Proyecto listo para usar!"