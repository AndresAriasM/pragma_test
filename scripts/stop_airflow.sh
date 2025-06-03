#!/bin/bash
echo "🛑 Parando servicios de Airflow..."

# Terminar sesiones tmux
tmux kill-session -t airflow-webserver 2>/dev/null && echo "✅ Webserver parado" || echo "ℹ️ Webserver no estaba corriendo"
tmux kill-session -t airflow-scheduler 2>/dev/null && echo "✅ Scheduler parado" || echo "ℹ️ Scheduler no estaba corriendo"

# Terminar procesos residuales
pkill -f "airflow webserver" 2>/dev/null || true
pkill -f "airflow scheduler" 2>/dev/null || true

echo "✅ Servicios de Airflow detenidos"
