#!/bin/bash
echo "🔄 Reiniciando Airflow..."
./scripts/stop_airflow.sh
sleep 3
./scripts/start_airflow_py311.sh
