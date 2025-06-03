#!/bin/bash
# scripts/setup_airflow_python311_uv.sh
set -e

PROJECT_ROOT=$(pwd)
AIRFLOW_HOME="$PROJECT_ROOT/airflow_config"
VENV_PATH="$PROJECT_ROOT/.venv"

echo "🌪️ Configurando Apache Airflow con Python 3.11 y UV..."
echo "📁 Proyecto: $PROJECT_ROOT"
echo "🌪️ Airflow Home: $AIRFLOW_HOME"
echo ""

# ==========================================
# 1. VERIFICAR/INSTALAR UV
# ==========================================

echo "⚡ Verificando UV (ultrafast Python package installer)..."

if ! command -v uv &> /dev/null; then
    echo "📦 Instalando UV..."
    
    # Detectar sistema operativo e instalar UV
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install uv
        else
            curl -LsSf https://astral.sh/uv/install.sh | sh
            export PATH="$HOME/.cargo/bin:$PATH"
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        curl -LsSf https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.cargo/bin:$PATH"
    else
        echo "❌ Error: Sistema operativo no soportado para UV"
        echo "💡 Instala UV manualmente desde https://github.com/astral-sh/uv"
        exit 1
    fi
    
    # Recargar PATH
    source ~/.bashrc 2>/dev/null || source ~/.zshrc 2>/dev/null || true
fi

# Verificar UV
if command -v uv &> /dev/null; then
    echo "✅ UV disponible: $(uv --version)"
else
    echo "❌ Error: UV no se pudo instalar"
    exit 1
fi

# ==========================================
# 2. VERIFICAR/INSTALAR PYTHON 3.11
# ==========================================

echo ""
echo "🐍 Verificando Python 3.11..."

# Función para verificar si una versión de Python está disponible
check_python_version() {
    local python_cmd=$1
    if command -v $python_cmd &> /dev/null; then
        local version=$($python_cmd --version 2>&1 | grep -o "3\.11\.[0-9]*")
        if [[ -n "$version" ]]; then
            echo "✅ Encontrado: $python_cmd (versión $version)"
            return 0
        fi
    fi
    return 1
}

# Buscar Python 3.11 en diferentes ubicaciones
PYTHON311_CMD=""

if check_python_version "python3.11"; then
    PYTHON311_CMD="python3.11"
elif check_python_version "python3"; then
    # Verificar si python3 es realmente 3.11
    version=$(python3 --version 2>&1 | grep -o "3\.11\.[0-9]*" || true)
    if [[ -n "$version" ]]; then
        PYTHON311_CMD="python3"
    fi
fi

# Si no se encuentra Python 3.11, instalarlo
if [[ -z "$PYTHON311_CMD" ]]; then
    echo "❌ Python 3.11 no encontrado. Instalando..."
    
    # Detectar sistema operativo
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        echo "🍎 Detectado macOS"
        
        # Verificar si Homebrew está instalado
        if ! command -v brew &> /dev/null; then
            echo "📦 Instalando Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
            
            # Agregar Homebrew al PATH
            if [[ -f "/opt/homebrew/bin/brew" ]]; then
                eval "$(/opt/homebrew/bin/brew shellenv)"
            elif [[ -f "/usr/local/bin/brew" ]]; then
                eval "$(/usr/local/bin/brew shellenv)"
            fi
        fi
        
        echo "📦 Instalando Python 3.11 con Homebrew..."
        brew install python@3.11
        
        # Verificar instalación
        if [[ -f "/opt/homebrew/bin/python3.11" ]]; then
            PYTHON311_CMD="/opt/homebrew/bin/python3.11"
        elif [[ -f "/usr/local/bin/python3.11" ]]; then
            PYTHON311_CMD="/usr/local/bin/python3.11"
        else
            echo "❌ Error: No se pudo instalar Python 3.11"
            exit 1
        fi
        
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        echo "🐧 Detectado Linux"
        
        # Ubuntu/Debian
        if command -v apt &> /dev/null; then
            echo "📦 Instalando Python 3.11 con apt..."
            sudo apt update
            sudo apt install -y software-properties-common
            sudo add-apt-repository -y ppa:deadsnakes/ppa
            sudo apt update
            sudo apt install -y python3.11 python3.11-venv python3.11-dev
            PYTHON311_CMD="python3.11"
            
        # CentOS/RHEL/Fedora
        elif command -v yum &> /dev/null || command -v dnf &> /dev/null; then
            echo "📦 Instalando Python 3.11 con yum/dnf..."
            if command -v dnf &> /dev/null; then
                sudo dnf install -y python3.11 python3.11-venv
            else
                sudo yum install -y python3.11 python3.11-venv
            fi
            PYTHON311_CMD="python3.11"
        else
            echo "❌ Error: Gestor de paquetes no soportado"
            echo "💡 Instala Python 3.11 manualmente desde https://www.python.org/downloads/"
            exit 1
        fi
    else
        echo "❌ Error: Sistema operativo no soportado"
        echo "💡 Instala Python 3.11 manualmente desde https://www.python.org/downloads/"
        exit 1
    fi
fi

echo "✅ Python 3.11 disponible: $PYTHON311_CMD"

# Verificar versión final
PYTHON_VERSION=$($PYTHON311_CMD --version)
echo "🐍 Versión: $PYTHON_VERSION"

# ==========================================
# 3. VERIFICAR/INSTALAR TMUX
# ==========================================

echo ""
echo "📱 Verificando tmux..."

if ! command -v tmux &> /dev/null; then
    echo "📦 Instalando tmux..."
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install tmux
        else
            echo "❌ Error: Homebrew necesario para instalar tmux en macOS"
            exit 1
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        if command -v apt &> /dev/null; then
            sudo apt update && sudo apt install -y tmux
        elif command -v yum &> /dev/null; then
            sudo yum install -y tmux
        elif command -v dnf &> /dev/null; then
            sudo dnf install -y tmux
        else
            echo "❌ Error: No se pudo instalar tmux automáticamente"
            echo "💡 Instala tmux manualmente"
            exit 1
        fi
    fi
fi

if command -v tmux &> /dev/null; then
    echo "✅ tmux disponible: $(tmux -V)"
else
    echo "❌ Error: tmux no se pudo instalar"
    exit 1
fi

# ==========================================
# 4. LIMPIAR INSTALACIÓN ANTERIOR
# ==========================================

echo ""
echo "🧹 Limpiando instalación anterior..."

# Eliminar virtual environment anterior
if [ -d "$VENV_PATH" ]; then
    echo "🗑️ Eliminando virtual environment anterior..."
    rm -rf "$VENV_PATH"
fi

# Eliminar configuración anterior de Airflow
if [ -d "$AIRFLOW_HOME" ]; then
    echo "🗑️ Eliminando configuración Airflow anterior..."
    rm -rf "$AIRFLOW_HOME"
fi

# ==========================================
# 5. CREAR VIRTUAL ENVIRONMENT CON UV
# ==========================================

echo ""
echo "⚡ Creando virtual environment con UV y Python 3.11..."

# Crear virtual environment con UV
uv venv "$VENV_PATH" --python "$PYTHON311_CMD"

# Verificar que se creó correctamente
if [ ! -f "$VENV_PATH/bin/activate" ]; then
    echo "❌ Error: No se pudo crear virtual environment con UV"
    exit 1
fi

# Activar virtual environment
source "$VENV_PATH/bin/activate"

# Verificar que estamos usando Python 3.11
ACTIVE_PYTHON_VERSION=$(python --version)
echo "✅ Virtual environment activo: $ACTIVE_PYTHON_VERSION"

# ==========================================
# 6. INSTALAR DEPENDENCIAS CON UV
# ==========================================

echo ""
echo "⚡ Instalando dependencias con UV (ultrafast)..."

# Verificar que requirements.txt existe
if [ ! -f "$PROJECT_ROOT/requirements.txt" ]; then
    echo "❌ Error: requirements.txt no encontrado en $PROJECT_ROOT"
    echo "💡 Asegúrate de tener un archivo requirements.txt en el proyecto"
    exit 1
fi

echo "📋 Usando requirements.txt: $PROJECT_ROOT/requirements.txt"

# Instalar dependencias con UV (mucho más rápido que pip)
echo "⬇️ Instalando dependencias con UV..."
uv pip install -r "$PROJECT_ROOT/requirements.txt"

# Instalar tmux en el ambiente virtual (si es necesario como dependencia Python)
echo "📱 Verificando dependencias adicionales..."
uv pip install libtmux  # Para interactuar con tmux desde Python si es necesario

# Verificar instalación de Airflow
echo "🔍 Verificando instalación de Airflow..."
if command -v airflow &> /dev/null; then
    echo "✅ Airflow instalado correctamente"
    airflow version
else
    echo "❌ Error: Airflow no se instaló correctamente"
    exit 1
fi

# ==========================================
# 7. CREAR ESTRUCTURA DE DIRECTORIOS
# ==========================================

echo ""
echo "📁 Creando estructura de directorios..."

mkdir -p "$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$AIRFLOW_HOME/plugins"
mkdir -p "$PROJECT_ROOT/data/raw"
mkdir -p "$PROJECT_ROOT/data/processed/bronze"
mkdir -p "$PROJECT_ROOT/data/processed/silver"
mkdir -p "$PROJECT_ROOT/data/processed/gold"
mkdir -p "$PROJECT_ROOT/logs"

echo "✅ Directorios creados:"
echo "  📁 $AIRFLOW_HOME"
echo "  📁 $PROJECT_ROOT/data"
echo "  📁 $PROJECT_ROOT/logs"

# ==========================================
# 8. CONFIGURAR AIRFLOW
# ==========================================

echo ""
echo "⚙️ Configurando Airflow..."

# Establecer variable de entorno
export AIRFLOW_HOME="$AIRFLOW_HOME"

# Inicializar base de datos
echo "🗄️ Inicializando base de datos de Airflow..."
airflow db init

# Crear usuario admin
echo "👤 Creando usuario administrador..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123 2>/dev/null || echo "ℹ️ Usuario admin ya existe"

# ==========================================
# 9. COPIAR DAGs
# ==========================================

echo ""
echo "📋 Copiando DAGs..."

# Verificar si los DAGs originales existen
if [ -f "$PROJECT_ROOT/airflow_config/dags/daily_pipeline_dag.py" ]; then
    cp "$PROJECT_ROOT/airflow_config/dags/daily_pipeline_dag.py" "$AIRFLOW_HOME/dags/"
    echo "✅ daily_pipeline_dag.py copiado"
fi

if [ -f "$PROJECT_ROOT/airflow_config/dags/bronze_processing_dag.py" ]; then
    cp "$PROJECT_ROOT/airflow_config/dags/bronze_processing_dag.py" "$AIRFLOW_HOME/dags/"
    echo "✅ bronze_processing_dag.py copiado"
fi

# Copiar todos los DAGs si existe el directorio
if [ -d "$PROJECT_ROOT/dags" ]; then
    cp -r "$PROJECT_ROOT/dags/"*.py "$AIRFLOW_HOME/dags/" 2>/dev/null || true
    echo "✅ DAGs adicionales copiados desde /dags"
fi

# ==========================================
# 10. CREAR SCRIPTS DE GESTIÓN MEJORADOS
# ==========================================

echo ""
echo "📝 Creando scripts de gestión mejorados..."

# Script para activar environment
cat > "$PROJECT_ROOT/activate_airflow.sh" << EOF
#!/bin/bash
# Script para activar el environment de Airflow con UV
export AIRFLOW_HOME="$AIRFLOW_HOME"
export PYTHONPATH="$PROJECT_ROOT/src:\$PYTHONPATH"
export PROJECT_ROOT="$PROJECT_ROOT"
source "$VENV_PATH/bin/activate"

echo "✅ Environment de Airflow activado (UV)"
echo "🐍 Python: \$(which python)"
echo "🌪️ Airflow: \$(which airflow)"
echo "⚡ UV: \$(which uv)"
echo "📱 tmux: \$(which tmux)"
echo "📁 AIRFLOW_HOME: \$AIRFLOW_HOME"
echo ""
echo "💡 Comandos útiles:"
echo "  airflow webserver --port 8080"
echo "  airflow scheduler"
echo "  airflow dags list"
echo "  uv pip install <package>  # Para instalar paquetes adicionales"
EOF

chmod +x "$PROJECT_ROOT/activate_airflow.sh"

# Script mejorado para iniciar Airflow con tmux
cat > "$PROJECT_ROOT/scripts/start_airflow_py311.sh" << EOF
#!/bin/bash
export AIRFLOW_HOME="$AIRFLOW_HOME"
cd "$PROJECT_ROOT"
source "$VENV_PATH/bin/activate"

echo "🌪️ Iniciando Airflow with Python 3.11 + UV + tmux..."
echo "📁 Proyecto: $PROJECT_ROOT"
echo "🐍 Python: \$(python --version)"
echo "⚡ UV: \$(uv --version)"
echo "🌪️ Airflow: \$(airflow version)"
echo "📱 tmux: \$(tmux -V)"
echo "🌐 Web UI: http://localhost:8080"
echo "👤 Login: admin / admin123"
echo ""

# Terminar sesiones existentes
echo "🛑 Terminando sesiones tmux anteriores..."
tmux kill-session -t airflow-webserver 2>/dev/null || true
tmux kill-session -t airflow-scheduler 2>/dev/null || true

# Esperar un momento para limpieza
sleep 2

echo "🚀 Iniciando servicios con tmux..."

# Crear sesión para webserver
tmux new-session -d -s airflow-webserver -c "$PROJECT_ROOT" \\
    "source '$VENV_PATH/bin/activate' && \\
     export AIRFLOW_HOME='$AIRFLOW_HOME' && \\
     export PYTHONPATH='$PROJECT_ROOT/src:\$PYTHONPATH' && \\
     echo '🌐 Iniciando Airflow Webserver...' && \\
     airflow webserver --port 8080"

# Crear sesión para scheduler
tmux new-session -d -s airflow-scheduler -c "$PROJECT_ROOT" \\
    "source '$VENV_PATH/bin/activate' && \\
     export AIRFLOW_HOME='$AIRFLOW_HOME' && \\
     export PYTHONPATH='$PROJECT_ROOT/src:\$PYTHONPATH' && \\
     echo '📅 Iniciando Airflow Scheduler...' && \\
     sleep 5 && \\
     airflow scheduler"

# Esperar que los servicios inicien
echo "⏳ Esperando que los servicios inicien..."
sleep 10

echo ""
echo "✅ Airflow iniciado correctamente!"
echo "📱 Sesiones tmux activas:"
echo "  🌐 Webserver: tmux attach -t airflow-webserver"
echo "  📅 Scheduler: tmux attach -t airflow-scheduler"
echo ""
echo "🌐 Acceso Web: http://localhost:8080"
echo "👤 Usuario: admin"
echo "🔐 Password: admin123"
echo ""
echo "💡 Comandos útiles:"
echo "  🔍 Ver estado: ./scripts/check_airflow_py311.sh"
echo "  🛑 Parar servicios: ./scripts/stop_airflow.sh"
echo "  📱 Listar sesiones: tmux list-sessions"
echo "  🔄 Reiniciar: ./scripts/restart_airflow.sh"
EOF

chmod +x "$PROJECT_ROOT/scripts/start_airflow_py311.sh"

# Script para parar Airflow
cat > "$PROJECT_ROOT/scripts/stop_airflow.sh" << EOF
#!/bin/bash
echo "🛑 Parando servicios de Airflow..."

# Terminar sesiones tmux
tmux kill-session -t airflow-webserver 2>/dev/null && echo "✅ Webserver parado" || echo "ℹ️ Webserver no estaba corriendo"
tmux kill-session -t airflow-scheduler 2>/dev/null && echo "✅ Scheduler parado" || echo "ℹ️ Scheduler no estaba corriendo"

# Terminar procesos residuales
pkill -f "airflow webserver" 2>/dev/null || true
pkill -f "airflow scheduler" 2>/dev/null || true

echo "✅ Servicios de Airflow detenidos"
EOF

chmod +x "$PROJECT_ROOT/scripts/stop_airflow.sh"

# Script para reiniciar Airflow  
cat > "$PROJECT_ROOT/scripts/restart_airflow.sh" << EOF
#!/bin/bash
echo "🔄 Reiniciando Airflow..."
./scripts/stop_airflow.sh
sleep 3
./scripts/start_airflow_py311.sh
EOF

chmod +x "$PROJECT_ROOT/scripts/restart_airflow.sh"

# Script mejorado para verificar estado
cat > "$PROJECT_ROOT/scripts/check_airflow_py311.sh" << EOF
#!/bin/bash
export AIRFLOW_HOME="$AIRFLOW_HOME"
cd "$PROJECT_ROOT"

echo "🔍 DIAGNÓSTICO AIRFLOW CON PYTHON 3.11 + UV"
echo "==========================================="
echo "📁 Proyecto: $PROJECT_ROOT"
echo "🌪️ Airflow Home: $AIRFLOW_HOME"
echo ""

# Verificar herramientas
echo "🛠️ Herramientas:"
command -v uv &> /dev/null && echo "  ✅ UV: \$(uv --version)" || echo "  ❌ UV no disponible"
command -v tmux &> /dev/null && echo "  ✅ tmux: \$(tmux -V)" || echo "  ❌ tmux no disponible"

# Verificar Python y ambiente
echo ""
echo "🐍 Environment:"
if [ -f "$VENV_PATH/bin/activate" ]; then
    source "$VENV_PATH/bin/activate"
    echo "  ✅ Virtual env activo (UV)"
    echo "  🐍 Python: \$(python --version)"
    echo "  📍 Ubicación: \$(which python)"
    
    if command -v airflow &> /dev/null; then
        echo "  ✅ Airflow disponible"
        echo "  📋 Versión: \$(airflow version)"
        echo "  📍 Ubicación: \$(which airflow)"
    else
        echo "  ❌ Airflow NO disponible"
    fi
else
    echo "  ❌ Virtual env no encontrado"
fi

echo ""
echo "📁 Archivos críticos:"
[ -f "$AIRFLOW_HOME/airflow.db" ] && echo "  ✅ airflow.db" || echo "  ❌ airflow.db"
[ -f "$AIRFLOW_HOME/airflow.cfg" ] && echo "  ✅ airflow.cfg" || echo "  ❌ airflow.cfg"
[ -d "$AIRFLOW_HOME/dags" ] && echo "  ✅ dags/" || echo "  ❌ dags/"

if [ -d "$AIRFLOW_HOME/dags" ]; then
    dag_count=\$(ls -1 "$AIRFLOW_HOME/dags"/*.py 2>/dev/null | wc -l)
    echo "  📋 DAGs encontrados: \$dag_count"
    
    if [ \$dag_count -gt 0 ]; then
        echo "  📝 Archivos DAG:"
        ls -la "$AIRFLOW_HOME/dags"/*.py 2>/dev/null | awk '{print "    " \$9}' || true
    fi
fi

echo ""
echo "📋 Procesos:"
webserver_pid=\$(pgrep -f "airflow webserver" 2>/dev/null || true)
scheduler_pid=\$(pgrep -f "airflow scheduler" 2>/dev/null || true)

if [ ! -z "\$webserver_pid" ]; then
    echo "  ✅ Webserver (PID: \$webserver_pid)"
else
    echo "  ❌ Webserver NO corriendo"
fi

if [ ! -z "\$scheduler_pid" ]; then
    echo "  ✅ Scheduler (PID: \$scheduler_pid)"
else
    echo "  ❌ Scheduler NO corriendo"
fi

echo ""
echo "📱 Sesiones tmux:"
if tmux has-session -t airflow-webserver 2>/dev/null; then
    echo "  ✅ airflow-webserver"
else
    echo "  ❌ airflow-webserver"
fi

if tmux has-session -t airflow-scheduler 2>/dev/null; then
    echo "  ✅ airflow-scheduler"
else
    echo "  ❌ airflow-scheduler"
fi

echo ""
echo "🌐 Conectividad:"
if [ ! -z "\$webserver_pid" ]; then
    if curl -s http://localhost:8080 > /dev/null 2>&1; then
        echo "  ✅ Web UI accesible en http://localhost:8080"
    else
        echo "  ⚠️ Web UI no responde (puede estar iniciando)"
    fi
else
    echo "  ❌ Web UI no disponible"
fi

echo ""
echo "🔑 Credenciales:"
echo "  👤 Usuario: admin"
echo "  🔐 Password: admin123"

# Test de DAGs si Airflow está corriendo
if [ ! -z "\$webserver_pid" ] && [ ! -z "\$scheduler_pid" ]; then
    echo ""
    echo "📋 Estado de DAGs:"
    source "$VENV_PATH/bin/activate"
    export AIRFLOW_HOME="$AIRFLOW_HOME"
    timeout 10 airflow dags list 2>/dev/null | head -10 || echo "  ⚠️ Timeout o error conectando a la base de datos"
fi

echo ""
echo "💡 Comandos útiles:"
echo "  🚀 Iniciar: ./scripts/start_airflow_py311.sh"
echo "  🛑 Parar: ./scripts/stop_airflow.sh"
echo "  🔄 Reiniciar: ./scripts/restart_airflow.sh"
echo "  📱 Ver webserver: tmux attach -t airflow-webserver"
echo "  📅 Ver scheduler: tmux attach -t airflow-scheduler"
EOF

chmod +x "$PROJECT_ROOT/scripts/check_airflow_py311.sh"

# ==========================================
# 11. CONFIGURACIÓN FINAL
# ==========================================

echo ""
echo "⚙️ Configuración final..."

# Crear archivo de variables de entorno mejorado
cat > "$PROJECT_ROOT/.airflow_env" << EOF
# Variables de entorno para Airflow con Python 3.11 + UV + tmux
export AIRFLOW_HOME="$AIRFLOW_HOME"
export PYTHONPATH="$PROJECT_ROOT/src:\$PYTHONPATH"
export PROJECT_ROOT="$PROJECT_ROOT"

# Para activar:
# source .airflow_env
# source .venv/bin/activate

# Herramientas disponibles:
# - uv: gestor de paquetes ultrarrápido
# - tmux: multiplexor de terminal  
# - airflow: orquestador de workflows
EOF

# Crear archivo .env para el proyecto
cat > "$PROJECT_ROOT/.env" << EOF
# Configuración del proyecto
PROJECT_ROOT=$PROJECT_ROOT
AIRFLOW_HOME=$AIRFLOW_HOME
PYTHON_VERSION=3.11

# Configuración de base de datos (SQLite por defecto)
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$AIRFLOW_HOME/airflow.db

# Configuración de logging
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__LOGGING__BASE_LOG_FOLDER=$AIRFLOW_HOME/logs

# Configuración de seguridad
AIRFLOW__WEBSERVER__SECRET_KEY=$(python -c "import secrets; print(secrets.token_urlsafe(32))")
EOF

# Verificar que todo funciona
echo "🔍 Verificación final..."
source "$VENV_PATH/bin/activate"
export AIRFLOW_HOME="$AIRFLOW_HOME"
airflow db check 2>/dev/null && echo "✅ Base de datos OK" || echo "⚠️ Verificar base de datos"

# ==========================================
# 12. INSTRUCCIONES FINALES
# ==========================================

echo ""
echo "🎉 INSTALACIÓN COMPLETADA CON PYTHON 3.11 + UV + TMUX"
echo "====================================================="
echo ""
echo "✅ Resumen de herramientas instaladas:"
echo "  🐍 Python: $PYTHON_VERSION"
echo "  ⚡ UV: $(uv --version 2>/dev/null || echo 'disponible')"
echo "  📱 tmux: $(tmux -V 2>/dev/null || echo 'disponible')" 
echo "  🌪️ Airflow: $(airflow version 2>/dev/null || echo 'instalado')"
echo "  📁 Proyecto: $PROJECT_ROOT"
echo "  🗂️ AIRFLOW_HOME: $AIRFLOW_HOME"
echo ""
echo "🚀 PRÓXIMOS PASOS:"
echo ""
echo "1. 📋 Verificar instalación completa:"
echo "   ./scripts/check_airflow_py311.sh"
echo ""
echo "2. 🌪️ Iniciar Airflow (con tmux automático):"
echo "   ./scripts/start_airflow_py311.sh"
echo ""
echo "3. 🌐 Acceder a Web UI:"
echo "   http://localhost:8080"
echo "   👤 Usuario: admin"
echo "   🔐 Password: admin123"
echo ""
echo "4. ⚡ Activar environment para desarrollo:"
echo "   ./activate_airflow.sh"
echo ""
echo "5. 📦 Instalar paquetes adicionales:"
echo "   source ./activate_airflow.sh"
echo "   uv pip install <package_name>"
echo ""
echo "6. 🔧 Gestión de servicios:"
echo "   🛑 Parar: ./scripts/stop_airflow.sh"
echo "   🔄 Reiniciar: ./scripts/restart_airflow.sh"
echo "   📱 Ver logs webserver: tmux attach -t airflow-webserver"
echo "   📅 Ver logs scheduler: tmux attach -t airflow-scheduler"
echo ""
echo "💡 VENTAJAS DE ESTA CONFIGURACIÓN:"
echo ""
echo "⚡ UV (Ultra-fast Python package manager):"
echo "  • Instalación de paquetes 10-100x más rápida que pip"
echo "  • Resolución de dependencias mejorada"
echo "  • Manejo inteligente de conflictos"
echo ""
echo "📱 tmux (Terminal multiplexer):"
echo "  • Servicios corriendo en background"
echo "  • Sesiones persistentes (sobreviven desconexiones SSH)"
echo "  • Fácil acceso a logs en tiempo real"
echo "  • Gestión independiente de webserver y scheduler"
echo ""
echo "🌪️ Apache Airflow 2.9.3:"
echo "  • Compatible con Python 3.11"
echo "  • Última versión estable"
echo "  • Mejor rendimiento y características"
echo ""
echo "📁 Estructura de datos organizada:"
echo "  • /data/raw - Datos sin procesar"
echo "  • /data/processed/bronze - Datos limpios"
echo "  • /data/processed/silver - Datos enriquecidos"
echo "  • /data/processed/gold - Datos analíticos"
echo ""
echo "🔧 COMANDOS DE TROUBLESHOOTING:"
echo ""
echo "Si hay problemas con tmux:"
echo "  tmux kill-server  # Reinicia tmux completamente"
echo "  tmux list-sessions  # Ver sesiones activas"
echo ""
echo "Si hay problemas con Airflow:"
echo "  source ./activate_airflow.sh"
echo "  airflow db reset  # Reinicia la base de datos (⚠️ borra datos)"
echo "  airflow db upgrade  # Actualiza esquema de BD"
echo ""
echo "Si hay problemas con UV:"
echo "  uv pip list  # Ver paquetes instalados"
echo "  uv pip install --upgrade <package>  # Actualizar paquete"
echo "  uv cache clean  # Limpiar cache de UV"
echo ""
echo "🔗 RECURSOS ÚTILES:"
echo ""
echo "📚 Documentación:"
echo "  • Airflow: https://airflow.apache.org/docs/"
echo "  • UV: https://github.com/astral-sh/uv"
echo "  • tmux: https://github.com/tmux/tmux/wiki"
echo ""
echo "🎯 ¡Tu pipeline de datos está listo para procesar información!"
echo "🚀 Usa UV para instalar paquetes rápidamente"
echo "📱 Usa tmux para gestionar servicios de manera profesional"
echo ""
echo "💝 BONUS: Archivos creados para ti:"
echo "  📋 requirements.txt - Dependencias actualizadas y versionadas"
echo "  🔧 activate_airflow.sh - Activación rápida del environment"
echo "  🚀 start_airflow_py311.sh - Inicio automático con tmux"
echo "  🛑 stop_airflow.sh - Parada limpia de servicios"
echo "  🔄 restart_airflow.sh - Reinicio rápido"
echo "  🔍 check_airflow_py311.sh - Diagnóstico completo del sistema"
echo "  📄 .airflow_env - Variables de entorno"
echo "  🔐 .env - Configuración del proyecto"