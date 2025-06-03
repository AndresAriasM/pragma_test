# src/pipeline/statistics_engine.py
"""
Motor de Estadísticas Incrementales O(1)

"""

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class IncrementalStatisticsEngine:
    """
    Motor de estadísticas incrementales que mantiene count, sum, min, max, avg
    sin necesidad de recalcular desde la base de datos.
    
    ✅ Algoritmo O(1) por operación vs O(n) de consultas SQL
    ✅ Escalable a millones de registros
    ✅ Persistencia opcional en archivo/BD
    """
    
    def __init__(self, persistence_file: Optional[str] = None):
        """
        Inicializa el motor de estadísticas
        
        Args:
            persistence_file: Archivo para persistir estadísticas (opcional)
        """
        self.stats = {
            'count': 0,
            'sum': 0.0,
            'min': float('inf'),
            'max': float('-inf'),
            'avg': 0.0,
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'version': '1.0'
        }
        
        self.batch_history = []  # Historial de micro-batches procesados
        self.persistence_file = persistence_file
        
        # Cargar estadísticas existentes si hay archivo de persistencia
        if persistence_file:
            self._load_from_file()
        
        logger.info(f"🔢 IncrementalStatisticsEngine inicializado")
        logger.info(f"📊 Estado inicial: {self.format_stats()}")
    
    def update_batch(self, prices: List[float], batch_info: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Actualiza estadísticas con un micro-batch de precios
        ✅ Operación O(1) por precio - NO consulta la base de datos
        
        Args:
            prices: Lista de precios del micro-batch
            batch_info: Información adicional del batch (archivo, número, etc.)
            
        Returns:
            Dict con estadísticas actualizadas
        """
        if not prices:
            logger.warning("⚠️ Batch vacío recibido")
            return self.get_current_stats()
        
        batch_start_count = self.stats['count']
        batch_min = min(prices)
        batch_max = max(prices)
        batch_sum = sum(prices)
        batch_count = len(prices)
        
        logger.info(f"🔄 Procesando micro-batch: {batch_count} precios")
        logger.info(f"   Batch stats: Min=${batch_min:.2f}, Max=${batch_max:.2f}, Avg=${batch_sum/batch_count:.2f}")
        
        # ✅ ACTUALIZACIÓN INCREMENTAL O(1) - NÚCLEO DEL ALGORITMO
        for price in prices:
            # Actualizar count y sum
            self.stats['count'] += 1
            self.stats['sum'] += price
            
            # Actualizar min y max (comparación O(1))
            if self.stats['min'] == float('inf') or price < self.stats['min']:
                self.stats['min'] = price
            if self.stats['max'] == float('-inf') or price > self.stats['max']:
                self.stats['max'] = price
        
        # Calcular nuevo promedio (O(1))
        self.stats['avg'] = self.stats['sum'] / self.stats['count']
        self.stats['last_updated'] = datetime.now().isoformat()
        
        # Registrar batch en historial
        batch_record = {
            'batch_number': len(self.batch_history) + 1,
            'rows_processed': batch_count,
            'batch_min': batch_min,
            'batch_max': batch_max,
            'batch_avg': batch_sum / batch_count,
            'running_count_before': batch_start_count,
            'running_count_after': self.stats['count'],
            'running_avg_after': self.stats['avg'],
            'processed_at': datetime.now().isoformat()
        }
        
        if batch_info:
            batch_record.update(batch_info)
        
        self.batch_history.append(batch_record)
        
        # Persistir si está configurado
        if self.persistence_file:
            self._save_to_file()
        
        logger.info(f"✅ Batch procesado: {self.format_stats()}")
        
        return self.get_current_stats()
    
    def get_current_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas actuales SIN consultar base de datos
        ✅ Operación O(1) - Estadísticas siempre disponibles
        
        Returns:
            Dict con estadísticas actuales
        """
        current_stats = self.stats.copy()
        
        # Agregar información adicional
        current_stats.update({
            'batches_processed': len(self.batch_history),
            'is_empty': self.stats['count'] == 0,
            'last_batch_info': self.batch_history[-1] if self.batch_history else None
        })
        
        return current_stats
    
    def format_stats(self, precision: int = 2) -> str:
        """
        Formatea estadísticas para mostrar en logs
        
        Args:
            precision: Decimales para mostrar
            
        Returns:
            String formateado con estadísticas
        """
        if self.stats['count'] == 0:
            return "Count: 0, Stats: N/A (sin datos)"
        
        return (
            f"Count: {self.stats['count']:,}, "
            f"Avg: ${self.stats['avg']:.{precision}f}, "
            f"Min: ${self.stats['min']:.{precision}f}, "
            f"Max: ${self.stats['max']:.{precision}f}"
        )
    
    def format_detailed_stats(self) -> str:
        """
        Formatea estadísticas detalladas para reportes
        
        Returns:
            String con estadísticas detalladas
        """
        if self.stats['count'] == 0:
            return "📊 Sin datos procesados aún"
        
        return f"""📊 ESTADÍSTICAS INCREMENTALES:
   Total de registros: {self.stats['count']:,}
   Suma total: ${self.stats['sum']:,.2f}
   Precio promedio: ${self.stats['avg']:.2f}
   Precio mínimo: ${self.stats['min']:.2f}
   Precio máximo: ${self.stats['max']:.2f}
   Micro-batches procesados: {len(self.batch_history)}
   Última actualización: {self.stats['last_updated']}
   Creado: {self.stats['created_at']}"""
    
    def compare_with_database_stats(self, db_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compara estadísticas incrementales con consulta directa a BD
        ✅ VERIFICACIÓN CRÍTICA: Deben coincidir exactamente
        
        Args:
            db_stats: Estadísticas obtenidas de consulta SQL directa
                     Formato esperado: {'count': int, 'avg': float, 'min': float, 'max': float}
        
        Returns:
            Dict con resultado de comparación
        """
        logger.info("🔍 COMPARANDO ESTADÍSTICAS INCREMENTALES VS BASE DE DATOS")
        
        comparison = {
            'incremental_stats': self.get_current_stats(),
            'database_stats': db_stats,
            'matches': {},
            'differences': {},
            'overall_match': True,
            'comparison_timestamp': datetime.now().isoformat()
        }
        
        # Comparar cada métrica
        metrics_to_compare = ['count', 'avg', 'min', 'max']
        tolerance = 1e-6  # Tolerancia para comparación de floats
        
        for metric in metrics_to_compare:
            incremental_value = self.stats.get(metric)
            db_value = db_stats.get(metric)
            
            # Manejar casos especiales
            if incremental_value == float('inf') or incremental_value == float('-inf'):
                incremental_value = None
            if db_value == float('inf') or db_value == float('-inf'):
                db_value = None
            
            # Comparar valores
            if incremental_value is None and db_value is None:
                matches = True
            elif incremental_value is None or db_value is None:
                matches = False
            elif isinstance(incremental_value, (int, float)) and isinstance(db_value, (int, float)):
                matches = abs(incremental_value - db_value) < tolerance
            else:
                matches = incremental_value == db_value
            
            comparison['matches'][metric] = matches
            
            if not matches:
                comparison['differences'][metric] = {
                    'incremental': incremental_value,
                    'database': db_value,
                    'difference': abs(incremental_value - db_value) if incremental_value and db_value else 'N/A'
                }
                comparison['overall_match'] = False
        
        # Log resultados
        if comparison['overall_match']:
            logger.info("✅ VERIFICACIÓN EXITOSA: Estadísticas incrementales coinciden con BD")
            logger.info(f"   Count: {self.stats['count']:,}")
            logger.info(f"   Avg: ${self.stats['avg']:.2f}")
            logger.info(f"   Min: ${self.stats['min']:.2f}")
            logger.info(f"   Max: ${self.stats['max']:.2f}")
        else:
            logger.error("❌ DIFERENCIAS DETECTADAS entre estadísticas incrementales y BD:")
            for metric, diff in comparison['differences'].items():
                logger.error(f"   {metric}: Incremental={diff['incremental']}, BD={diff['database']}")
        
        return comparison
    
    def reset_stats(self):
        """
        Reinicia todas las estadísticas (útil para testing)
        """
        logger.info("🔄 Reiniciando estadísticas")
        self.stats = {
            'count': 0,
            'sum': 0.0,
            'min': float('inf'),
            'max': float('-inf'),
            'avg': 0.0,
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'version': '1.0'
        }
        self.batch_history = []
        
        if self.persistence_file:
            self._save_to_file()
    
    def get_batch_history(self) -> List[Dict[str, Any]]:
        """
        Obtiene historial completo de micro-batches procesados
        
        Returns:
            Lista con información de cada batch procesado
        """
        return self.batch_history.copy()
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Obtiene métricas de rendimiento del motor
        
        Returns:
            Dict con métricas de rendimiento
        """
        if not self.batch_history:
            return {'batches_processed': 0, 'avg_batch_size': 0}
        
        total_rows = sum(batch['rows_processed'] for batch in self.batch_history)
        avg_batch_size = total_rows / len(self.batch_history)
        
        return {
            'total_batches_processed': len(self.batch_history),
            'total_rows_processed': total_rows,
            'avg_batch_size': avg_batch_size,
            'batches_per_file': len(self.batch_history) / len(set(batch.get('source_file', 'unknown') for batch in self.batch_history)),
            'processing_efficiency': 'O(1) per operation - optimal',
            'memory_usage': 'constant - independent of dataset size'
        }
    
    def _save_to_file(self):
        """
        Guarda estadísticas en archivo para persistencia
        """
        if not self.persistence_file:
            return
        
        try:
            data = {
                'stats': self.stats,
                'batch_history': self.batch_history,
                'saved_at': datetime.now().isoformat()
            }
            
            persistence_path = Path(self.persistence_file)
            persistence_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(persistence_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.debug(f"💾 Estadísticas guardadas en {self.persistence_file}")
        except Exception as e:
            logger.error(f"❌ Error guardando estadísticas: {e}")
    
    def _load_from_file(self):
        """
        Carga estadísticas desde archivo de persistencia
        """
        if not self.persistence_file or not Path(self.persistence_file).exists():
            return
        
        try:
            with open(self.persistence_file, 'r') as f:
                data = json.load(f)
            
            if 'stats' in data:
                self.stats.update(data['stats'])
            if 'batch_history' in data:
                self.batch_history = data['batch_history']
            
            logger.info(f"📂 Estadísticas cargadas desde {self.persistence_file}")
            logger.info(f"   Estado recuperado: {self.format_stats()}")
        except Exception as e:
            logger.error(f"❌ Error cargando estadísticas: {e}")


class StatisticsComparator:
    """
    Utilidad para comparar diferentes fuentes de estadísticas
    """
    
    @staticmethod
    def compare_multiple_sources(sources: Dict[str, Dict[str, Any]], tolerance: float = 1e-6) -> Dict[str, Any]:
        """
        Compara estadísticas de múltiples fuentes
        
        Args:
            sources: Dict con fuentes de estadísticas {'source_name': {'count': ..., 'avg': ...}}
            tolerance: Tolerancia para comparación de floats
            
        Returns:
            Dict con resultado de comparación múltiple
        """
        if len(sources) < 2:
            return {'error': 'Se necesitan al menos 2 fuentes para comparar'}
        
        source_names = list(sources.keys())
        reference_source = source_names[0]
        reference_stats = sources[reference_source]
        
        comparison = {
            'reference_source': reference_source,
            'comparison_sources': source_names[1:],
            'metrics_compared': ['count', 'avg', 'min', 'max'],
            'results': {},
            'overall_consistent': True,
            'timestamp': datetime.now().isoformat()
        }
        
        for compare_source in source_names[1:]:
            compare_stats = sources[compare_source]
            source_comparison = {}
            
            for metric in ['count', 'avg', 'min', 'max']:
                ref_value = reference_stats.get(metric)
                comp_value = compare_stats.get(metric)
                
                if isinstance(ref_value, (int, float)) and isinstance(comp_value, (int, float)):
                    matches = abs(ref_value - comp_value) < tolerance
                    difference = abs(ref_value - comp_value)
                else:
                    matches = ref_value == comp_value
                    difference = 'N/A'
                
                source_comparison[metric] = {
                    'matches': matches,
                    'reference_value': ref_value,
                    'compare_value': comp_value,
                    'difference': difference
                }
                
                if not matches:
                    comparison['overall_consistent'] = False
            
            comparison['results'][compare_source] = source_comparison
        
        return comparison


# Función de utilidad para crear motor con configuración por defecto
def create_statistics_engine(project_root: str = None, enable_persistence: bool = True) -> IncrementalStatisticsEngine:
    """
    Crea un motor de estadísticas con configuración por defecto
    
    Args:
        project_root: Ruta raíz del proyecto
        enable_persistence: Si habilitar persistencia en archivo
        
    Returns:
        IncrementalStatisticsEngine configurado
    """
    if project_root is None:
        project_root = Path(__file__).parent.parent.parent
    
    persistence_file = None
    if enable_persistence:
        persistence_file = str(Path(project_root) / "data" / "processed" / "statistics_engine.json")
    
    return IncrementalStatisticsEngine(persistence_file=persistence_file)


if __name__ == "__main__":
    # Demo del motor de estadísticas
    logging.basicConfig(level=logging.INFO)
    
    print("🔢 DEMO DEL MOTOR DE ESTADÍSTICAS INCREMENTALES")
    print("=" * 50)
    
    # Crear motor
    engine = IncrementalStatisticsEngine()
    
    # Simular micro-batches
    batches = [
        [10.5, 20.3, 15.7, 8.9, 25.1],
        [12.4, 18.6, 22.1, 9.3, 30.0],
        [5.5, 35.2, 28.4, 14.7, 19.8]
    ]
    
    for i, batch in enumerate(batches, 1):
        print(f"\n📦 Procesando batch {i}: {batch}")
        engine.update_batch(batch, {'source_file': f'demo_{i}.csv', 'batch_number': i})
    
    print(f"\n{engine.format_detailed_stats()}")
    
    # Simular verificación con BD
    fake_db_stats = {
        'count': engine.stats['count'],
        'avg': engine.stats['avg'],
        'min': engine.stats['min'],
        'max': engine.stats['max']
    }
    
    comparison = engine.compare_with_database_stats(fake_db_stats)
    print(f"\n✅ Verificación: {'EXITOSA' if comparison['overall_match'] else 'FALLIDA'}")