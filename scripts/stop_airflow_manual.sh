#!/bin/bash
echo "🛑 Parando Airflow..."

webserver_pid=$(cat "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/webserver_manual.pid" 2>/dev/null)
scheduler_pid=$(cat "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/scheduler_manual.pid" 2>/dev/null)

[ ! -z "$webserver_pid" ] && kill $webserver_pid 2>/dev/null && echo "✅ Webserver parado"
[ ! -z "$scheduler_pid" ] && kill $scheduler_pid 2>/dev/null && echo "✅ Scheduler parado"

rm -f "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/webserver_manual.pid"
rm -f "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/scheduler_manual.pid"

echo "✅ Airflow detenido"
