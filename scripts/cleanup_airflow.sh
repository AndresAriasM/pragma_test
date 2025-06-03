#!/bin/bash
# scripts/force_cleanup_airflow.sh - Limpieza forzada y completa

PROJECT_ROOT=$(pwd)
AIRFLOW_HOME="$PROJECT_ROOT/airflow_config"

echo "💥 LIMPIEZA FORZADA DE AIRFLOW"
echo "============================="
echo ""

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${RED}⚠️ ADVERTENCIA: Esto va a terminar TODOS los procesos de Airflow${NC}"
echo -e "${YELLOW}Presiona ENTER para continuar o Ctrl+C para cancelar...${NC}"
read

echo ""
echo -e "${BLUE}1️⃣ Terminando TODOS los procesos de Airflow (fuerza bruta)...${NC}"

# Matar TODOS los procesos relacionados con Airflow
pkill -9 -f "airflow" 2>/dev/null || true
pkill -9 -f "gunicorn.*airflow" 2>/dev/null || true
pkill -9 -f "python.*airflow" 2>/dev/null || true

# Matar procesos específicos en puertos
for port in 8080 8081 5555; do
    pid=$(lsof -ti:$port 2>/dev/null || true)
    if [ ! -z "$pid" ]; then
        echo -e "${YELLOW}💀 Matando proceso en puerto $port (PID: $pid)${NC}"
        kill -9 $pid 2>/dev/null || true
    fi
done

echo -e "${GREEN}✅ Procesos terminados${NC}"

echo ""
echo -e "${BLUE}2️⃣ Limpiando sesiones tmux...${NC}"

# Matar tmux servidor completo (nuclear option)
tmux kill-server 2>/dev/null || true
echo -e "${GREEN}✅ tmux reiniciado${NC}"

echo ""
echo -e "${BLUE}3️⃣ Limpiando archivos PID y locks...${NC}"

# Eliminar archivos PID
find "$AIRFLOW_HOME" -name "*.pid" -delete 2>/dev/null || true
find "$AIRFLOW_HOME" -name "airflow-webserver.pid" -delete 2>/dev/null || true
find "$AIRFLOW_HOME" -name "airflow-scheduler.pid" -delete 2>/dev/null || true

# Eliminar archivos de lock
find "$AIRFLOW_HOME" -name "*.lock" -delete 2>/dev/null || true

# Eliminar sockets
find /tmp -name "*airflow*" -name "*.sock" -delete 2>/dev/null || true
find /var/tmp -name "*airflow*" -name "*.sock" -delete 2>/dev/null || true

# Limpiar archivos temporales del sistema
find /tmp -name "*airflow*" -delete 2>/dev/null || true

echo -e "${GREEN}✅ Archivos de bloqueo eliminados${NC}"

echo ""
echo -e "${BLUE}4️⃣ Limpiando logs problemáticos...${NC}"

# Rotar logs grandes
if [ -d "$AIRFLOW_HOME/logs" ]; then
    find "$AIRFLOW_HOME/logs" -name "*.log" -size +50M -delete 2>/dev/null || true
    find "$AIRFLOW_HOME/logs" -name "*.log.1" -delete 2>/dev/null || true
fi

echo -e "${GREEN}✅ Logs limpiados${NC}"

echo ""
echo -e "${BLUE}5️⃣ Verificando limpieza...${NC}"

# Verificar que no hay procesos
remaining_processes=$(pgrep -f "airflow" 2>/dev/null | wc -l || echo 0)
remaining_ports=$(lsof -ti:8080,8081,5555 2>/dev/null | wc -l || echo 0)

if [ "$remaining_processes" -eq 0 ] && [ "$remaining_ports" -eq 0 ]; then
    echo -e "${GREEN}🎉 LIMPIEZA EXITOSA - Sistema completamente limpio${NC}"
else
    echo -e "${RED}⚠️ Advertencia: Procesos restantes: $remaining_processes, Puertos ocupados: $remaining_ports${NC}"
    
    if [ "$remaining_processes" -gt 0 ]; then
        echo -e "${YELLOW}Procesos restantes:${NC}"
        pgrep -f "airflow" 2>/dev/null || true
    fi
    
    if [ "$remaining_ports" -gt 0 ]; then
        echo -e "${YELLOW}Puertos ocupados:${NC}"
        lsof -i:8080,8081,5555 2>/dev/null || true
    fi
fi

echo ""
echo -e "${BLUE}6️⃣ Configurando para inicio limpio...${NC}"

# Asegurar permisos correctos
chmod -R 755 "$AIRFLOW_HOME" 2>/dev/null || true

# Crear directorios necesarios si no existen
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/plugins"

echo -e "${GREEN}✅ Configuración lista${NC}"

echo ""
echo -e "${GREEN}🚀 SISTEMA LISTO PARA INICIO LIMPIO${NC}"
echo "=================================="
echo ""
echo -e "${BLUE}Próximos pasos recomendados:${NC}"
echo "1. Espera 5 segundos para que el sistema se estabilice"
echo "2. Ejecuta: ./scripts/start_airflow_py311.sh"
echo "3. Si aún hay problemas, reinicia la terminal completa"
echo ""
echo -e "${YELLOW}💡 TIP: Para evitar problemas futuros:${NC}"
echo "• Usa siempre ./scripts/stop_airflow.sh antes de cerrar"
echo "• No cierres la terminal sin parar Airflow correctamente"
echo "• Si dudas, ejecuta este script de limpieza antes de iniciar"

# Pausa para estabilización
echo ""
echo -e "${BLUE}⏳ Esperando 5 segundos para estabilización del sistema...${NC}"
sleep 5
echo -e "${GREEN}✅ Listo para iniciar Airflow${NC}"