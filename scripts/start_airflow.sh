#!/bin/bash
# scripts/start_airflow_single.sh - Inicio único con lock para evitar duplicados

PROJECT_ROOT=$(pwd)
AIRFLOW_HOME="$PROJECT_ROOT/airflow_config"
VENV_PATH="$PROJECT_ROOT/.venv"
LOCK_FILE="$PROJECT_ROOT/.airflow_starting.lock"

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🌪️ INICIO ÚNICO DE AIRFLOW${NC}"
echo "=========================="

# Función de limpieza al salir
cleanup() {
    rm -f "$LOCK_FILE" 2>/dev/null
}
trap cleanup EXIT

# Verificar si ya hay un inicio en progreso
if [ -f "$LOCK_FILE" ]; then
    echo -e "${YELLOW}⚠️ Otro proceso de inicio ya está en ejecución${NC}"
    echo "Archivo lock: $LOCK_FILE"
    echo ""
    echo "Si esto es un error, elimina el lock:"
    echo "rm $LOCK_FILE"
    exit 1
fi

# Crear archivo lock
echo $$ > "$LOCK_FILE"
echo -e "${GREEN}🔒 Lock creado (PID: $$)${NC}"

echo ""
echo -e "${BLUE}1️⃣ Verificando estado actual...${NC}"

# Verificar si Airflow ya está corriendo
webserver_pid=""
scheduler_pid=""

if [ -f "$AIRFLOW_HOME/airflow-webserver.pid" ]; then
    webserver_pid=$(cat "$AIRFLOW_HOME/airflow-webserver.pid" 2>/dev/null)
fi

if [ -f "$AIRFLOW_HOME/airflow-scheduler.pid" ]; then
    scheduler_pid=$(cat "$AIRFLOW_HOME/airflow-scheduler.pid" 2>/dev/null)
fi

# Verificar si los PIDs están realmente corriendo
webserver_running=false
scheduler_running=false

if [ ! -z "$webserver_pid" ] && kill -0 "$webserver_pid" 2>/dev/null; then
    webserver_running=true
    echo -e "${GREEN}✅ Webserver ya corriendo (PID: $webserver_pid)${NC}"
fi

if [ ! -z "$scheduler_pid" ] && kill -0 "$scheduler_pid" 2>/dev/null; then
    scheduler_running=true
    echo -e "${GREEN}✅ Scheduler ya corriendo (PID: $scheduler_pid)${NC}"
fi

# Si ambos están corriendo, mostrar info y salir
if [ "$webserver_running" = true ] && [ "$scheduler_running" = true ]; then
    echo ""
    echo -e "${GREEN}🎉 AIRFLOW YA ESTÁ CORRIENDO${NC}"
    echo "============================"
    
    # Encontrar puerto del webserver
    port=$(lsof -p "$webserver_pid" -i | grep LISTEN | awk '{print $9}' | cut -d: -f2 | head -1)
    echo -e "${BLUE}🌐 Web UI:${NC} http://localhost:${port:-8080}"
    echo -e "${BLUE}👤 Usuario:${NC} admin"
    echo -e "${BLUE}🔐 Password:${NC} admin123"
    echo ""
    echo -e "${YELLOW}💡 Para parar:${NC} ./scripts/stop_airflow.sh"
    echo -e "${YELLOW}💡 Para reiniciar:${NC} ./scripts/restart_airflow.sh"
    
    cleanup
    exit 0
fi

echo ""
echo -e "${BLUE}2️⃣ Limpiando archivos PID obsoletos...${NC}"

# Limpiar PIDs obsoletos (procesos que ya no existen)
if [ ! -z "$webserver_pid" ] && ! kill -0 "$webserver_pid" 2>/dev/null; then
    echo -e "${YELLOW}🧹 Eliminando PID obsoleto webserver: $webserver_pid${NC}"
    rm -f "$AIRFLOW_HOME/airflow-webserver.pid"
    webserver_running=false
fi

if [ ! -z "$scheduler_pid" ] && ! kill -0 "$scheduler_pid" 2>/dev/null; then
    echo -e "${YELLOW}🧹 Eliminando PID obsoleto scheduler: $scheduler_pid${NC}"
    rm -f "$AIRFLOW_HOME/airflow-scheduler.pid"
    scheduler_running=false
fi

echo ""
echo -e "${BLUE}3️⃣ Configurando environment...${NC}"

# Activar virtual environment
if [ ! -f "$VENV_PATH/bin/activate" ]; then
    echo -e "${RED}❌ Virtual environment no encontrado${NC}"
    cleanup
    exit 1
fi

source "$VENV_PATH/bin/activate"
export AIRFLOW_HOME="$AIRFLOW_HOME"
export PYTHONPATH="$PROJECT_ROOT/src:$PYTHONPATH"

echo -e "${GREEN}✅ Environment configurado${NC}"
echo "  🐍 Python: $(python --version)"
echo "  🌪️ Airflow: $(airflow version 2>/dev/null || echo 'Error')"

echo ""
echo -e "${BLUE}4️⃣ Verificando base de datos...${NC}"

# Verificar/inicializar base de datos
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo -e "${YELLOW}🗄️ Inicializando base de datos...${NC}"
    airflow db init
    
    echo -e "${BLUE}👤 Creando usuario admin...${NC}"
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin123 2>/dev/null || echo -e "${YELLOW}ℹ️ Usuario admin ya existe${NC}"
else
    echo -e "${GREEN}✅ Base de datos OK${NC}"
fi

echo ""
echo -e "${BLUE}5️⃣ Encontrando puerto disponible...${NC}"

# Encontrar puerto libre
AIRFLOW_PORT=""
for port in 8080 8081 8082 8083 8084; do
    if ! lsof -i:$port &> /dev/null; then
        AIRFLOW_PORT=$port
        break
    fi
done

if [ -z "$AIRFLOW_PORT" ]; then
    echo -e "${RED}❌ No se encontró puerto libre${NC}"
    cleanup
    exit 1
fi

echo -e "${GREEN}✅ Puerto disponible: $AIRFLOW_PORT${NC}"

echo ""
echo -e "${BLUE}6️⃣ Iniciando servicios...${NC}"

# Crear directorios de logs
mkdir -p "$AIRFLOW_HOME/logs"

# Iniciar webserver solo si no está corriendo
if [ "$webserver_running" = false ]; then
    echo -e "${YELLOW}🚀 Iniciando webserver en puerto $AIRFLOW_PORT...${NC}"
    
    # Usar nohup en lugar de tmux para evitar conflictos
    nohup airflow webserver --port "$AIRFLOW_PORT" \
        > "$AIRFLOW_HOME/logs/webserver.log" 2>&1 &
    
    webserver_pid=$!
    echo $webserver_pid > "$AIRFLOW_HOME/webserver_manual.pid"
    
    echo -e "${GREEN}✅ Webserver iniciado (PID: $webserver_pid)${NC}"
    
    # Esperar un poco para que se establezca
    sleep 5
else
    echo -e "${GREEN}✅ Webserver ya estaba corriendo${NC}"
fi

# Iniciar scheduler solo si no está corriendo
if [ "$scheduler_running" = false ]; then
    echo -e "${YELLOW}📅 Iniciando scheduler...${NC}"
    
    nohup airflow scheduler \
        > "$AIRFLOW_HOME/logs/scheduler.log" 2>&1 &
    
    scheduler_pid=$!
    echo $scheduler_pid > "$AIRFLOW_HOME/scheduler_manual.pid"
    
    echo -e "${GREEN}✅ Scheduler iniciado (PID: $scheduler_pid)${NC}"
else
    echo -e "${GREEN}✅ Scheduler ya estaba corriendo${NC}"
fi

echo ""
echo -e "${BLUE}7️⃣ Verificando servicios...${NC}"

# Esperar que los servicios inicien
echo -e "${YELLOW}⏳ Esperando que los servicios se estabilicen (15 segundos)...${NC}"

for i in {1..15}; do
    printf "."
    sleep 1
    
    # Verificar si el webserver responde
    if [ $i -gt 8 ] && curl -s -o /dev/null -w "%{http_code}" http://localhost:$AIRFLOW_PORT | grep -q "200\|401\|302"; then
        echo ""
        echo -e "${GREEN}✅ Webserver respondiendo!${NC}"
        break
    fi
done

echo ""

# Verificación final
webserver_responsive=false
if curl -s -o /dev/null -w "%{http_code}" http://localhost:$AIRFLOW_PORT | grep -q "200\|401\|302"; then
    webserver_responsive=true
fi

echo ""
echo -e "${BLUE}📊 ESTADO FINAL${NC}"
echo "==============="

# Leer PIDs finales
final_webserver_pid=$(cat "$AIRFLOW_HOME/webserver_manual.pid" 2>/dev/null || echo "$webserver_pid")
final_scheduler_pid=$(cat "$AIRFLOW_HOME/scheduler_manual.pid" 2>/dev/null || echo "$scheduler_pid")

# Verificar que los procesos están vivos
webserver_alive=$(kill -0 "$final_webserver_pid" 2>/dev/null && echo "true" || echo "false")
scheduler_alive=$(kill -0 "$final_scheduler_pid" 2>/dev/null && echo "true" || echo "false")

echo -e "Webserver (PID $final_webserver_pid): $([ "$webserver_alive" = "true" ] && echo -e "${GREEN}✅ Corriendo${NC}" || echo -e "${RED}❌ Parado${NC}")"
echo -e "Scheduler (PID $final_scheduler_pid): $([ "$scheduler_alive" = "true" ] && echo -e "${GREEN}✅ Corriendo${NC}" || echo -e "${RED}❌ Parado${NC}")"
echo -e "Web UI responde: $([ "$webserver_responsive" = "true" ] && echo -e "${GREEN}✅ Sí${NC}" || echo -e "${YELLOW}⏳ Cargando...${NC}")"

if [ "$webserver_alive" = "true" ] && [ "$scheduler_alive" = "true" ]; then
    echo ""
    echo -e "${GREEN}🎉 ¡AIRFLOW INICIADO EXITOSAMENTE!${NC}"
    echo "================================="
    echo ""
    echo -e "${BLUE}🌐 Acceso:${NC} http://localhost:$AIRFLOW_PORT"
    echo -e "${BLUE}👤 Usuario:${NC} admin"
    echo -e "${BLUE}🔐 Password:${NC} admin123"
    echo ""
    echo -e "${BLUE}📄 Logs en tiempo real:${NC}"
    echo "  tail -f $AIRFLOW_HOME/logs/webserver.log"
    echo "  tail -f $AIRFLOW_HOME/logs/scheduler.log"
    echo ""
    echo -e "${BLUE}🛑 Para parar:${NC}"
    echo "  kill $final_webserver_pid $final_scheduler_pid"
    echo "  O usa: ./scripts/stop_airflow_manual.sh"
    echo ""
    echo -e "${BLUE}🔍 Estado:${NC} ./scripts/check_airflow_py311.sh"
    
    # Crear script de parada
    cat > "$PROJECT_ROOT/scripts/stop_airflow_manual.sh" << EOF
#!/bin/bash
echo "🛑 Parando Airflow..."

webserver_pid=\$(cat "$AIRFLOW_HOME/webserver_manual.pid" 2>/dev/null)
scheduler_pid=\$(cat "$AIRFLOW_HOME/scheduler_manual.pid" 2>/dev/null)

[ ! -z "\$webserver_pid" ] && kill \$webserver_pid 2>/dev/null && echo "✅ Webserver parado"
[ ! -z "\$scheduler_pid" ] && kill \$scheduler_pid 2>/dev/null && echo "✅ Scheduler parado"

rm -f "$AIRFLOW_HOME/webserver_manual.pid"
rm -f "$AIRFLOW_HOME/scheduler_manual.pid"

echo "✅ Airflow detenido"
EOF
    chmod +x "$PROJECT_ROOT/scripts/stop_airflow_manual.sh"
    
else
    echo ""
    echo -e "${RED}❌ PROBLEMAS EN EL INICIO${NC}"
    echo "========================"
    echo ""
    echo -e "${YELLOW}🔍 Revisar logs:${NC}"
    echo "  tail -20 $AIRFLOW_HOME/logs/webserver.log"
    echo "  tail -20 $AIRFLOW_HOME/logs/scheduler.log"
fi

echo ""
if [ "$webserver_responsive" = "false" ]; then
    echo -e "${YELLOW}💡 Si el webserver no responde aún, espera 1-2 minutos más.${NC}"
    echo -e "${YELLOW}   Airflow puede tardar en cargar completamente.${NC}"
fi

echo -e "${BLUE}💾 PIDs guardados en:${NC}"
echo "  $AIRFLOW_HOME/webserver_manual.pid"
echo "  $AIRFLOW_HOME/scheduler_manual.pid"

cleanup