# src/pipeline/database_setup.py - VERSIÓN CORREGIDA
"""
Database Manager para el pipeline de datos - CORREGIDO
✅ Maneja SQLite/PostgreSQL con esquemas optimizados
✅ FIXED: Problema de conexiones SQLAlchemy vs SQLite nativo
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
import uuid

try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    logging.warning("SQLAlchemy no disponible, usando SQLite nativo")

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Maneja la base de datos para el pipeline de datos.
    ✅ FIXED: Separación clara entre SQLAlchemy y SQLite nativo
    """
    
    def __init__(self, database_config: Optional[Dict[str, Any]] = None):
        """
        Inicializa el gestor de base de datos
        """
        self.config = database_config or self._get_default_config()
        self.engine = None
        self.session_maker = None
        self.sqlite_connection = None  # ✅ RENAMED para claridad
        self.metadata = MetaData()
        self.use_sqlalchemy = False    # ✅ FLAG para saber qué motor usar
        
        # Configurar tablas
        self._define_tables()
        
        # Conectar a la base de datos
        self._connect()
        
        logger.info(f"🗄️ DatabaseManager inicializado")
        logger.info(f"   Tipo: {self.config['type']}")
        logger.info(f"   Motor: {'SQLAlchemy' if self.use_sqlalchemy else 'SQLite nativo'}")
        logger.info(f"   Ubicación: {self.config.get('path', self.config.get('host', 'N/A'))}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """
        Configuración por defecto (SQLite)
        """
        project_root = Path(__file__).parent.parent.parent
        db_path = project_root / "data" / "pipeline.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        return {
            'type': 'sqlite',
            'path': str(db_path),
            'echo': False
        }
    
    def _define_tables(self):
        """
        Define el esquema de tablas optimizado para el pipeline
        ✅ FIXED: Esquema más permisivo para timestamps
        """
        # Tabla principal de transacciones
        self.transactions_table = Table(
            'transactions',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('timestamp', String(50), nullable=False, index=True),
            Column('price', Float, nullable=False, index=True),
            Column('user_id', String(50), nullable=False, index=True),
            Column('source_file', String(100), nullable=False, index=True),
            Column('batch_id', String(50), nullable=True, index=True),
            # ✅ CAMBIAR DateTime a String para evitar problemas de conversión
            Column('bronze_created_at', String(50), nullable=True),  # Cambio: String en lugar de DateTime
            Column('db_inserted_at', String(50), nullable=False),   # Cambio: String en lugar de DateTime
            Column('processing_metadata', Text, nullable=True)
        )
        
        # Tabla de metadatos de batches
        self.batch_metadata_table = Table(
            'batch_metadata',
            self.metadata,
            Column('batch_id', String(50), primary_key=True),
            Column('source_file', String(100), nullable=False),
            Column('batch_number', Integer, nullable=False),
            Column('rows_processed', Integer, nullable=False),
            # ✅ CAMBIAR DateTime a String para consistencia
            Column('processing_start', String(50), nullable=True),  # Cambio: String
            Column('processing_end', String(50), nullable=True),    # Cambio: String
            Column('status', String(20), default='pending', nullable=False),
            Column('error_message', Text, nullable=True),
            Column('stats_snapshot', Text, nullable=True),
            Column('created_at', String(50), nullable=False)        # Cambio: String
        )
        
        # Tabla de verificaciones de estadísticas
        self.stats_verification_table = Table(
            'stats_verification',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('verification_timestamp', String(50), nullable=False),  # Cambio: String
            Column('incremental_stats', Text, nullable=False),
            Column('database_stats', Text, nullable=False),
            Column('comparison_result', Text, nullable=False),
            Column('verification_passed', Integer, nullable=False),
            Column('notes', Text, nullable=True)
        )
    
    def _connect(self):
        """
        Establece conexión con la base de datos
        ✅ FIXED: Lógica de conexión clarificada
        """
        try:
            if self.config['type'] == 'sqlite':
                self._connect_sqlite()
            elif self.config['type'] == 'postgresql':
                self._connect_postgresql()
            else:
                raise ValueError(f"Tipo de BD no soportado: {self.config['type']}")
            
            # Crear tablas según el motor usado
            if self.use_sqlalchemy:
                self.metadata.create_all(self.engine)
            else:
                self._create_tables_native_sqlite()
            
            logger.info("✅ Conexión a BD establecida y tablas verificadas")
            
        except Exception as e:
            logger.error(f"❌ Error conectando a BD: {e}")
            raise
    
    def _connect_sqlite(self):
        """
        Conecta a SQLite - FIXED
        """
        if SQLALCHEMY_AVAILABLE:
            # ✅ USAR SQLAlchemy
            connection_string = f"sqlite:///{self.config['path']}"
            self.engine = create_engine(
                connection_string,
                echo=self.config.get('echo', False)
            )
            self.session_maker = sessionmaker(bind=self.engine)
            self.use_sqlalchemy = True
            logger.info("✅ Usando SQLAlchemy para SQLite")
        else:
            # ✅ FALLBACK a SQLite nativo
            self.sqlite_connection = sqlite3.connect(self.config['path'])
            self.use_sqlalchemy = False
            logger.info("✅ Usando SQLite nativo")
    
    def _connect_postgresql(self):
        """
        Conecta a PostgreSQL (requiere SQLAlchemy)
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError("SQLAlchemy requerido para PostgreSQL")
        
        connection_string = (
            f"postgresql://{self.config['username']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
        
        self.engine = create_engine(
            connection_string,
            echo=self.config.get('echo', False),
            pool_size=self.config.get('pool_size', 5),
            max_overflow=self.config.get('max_overflow', 10)
        )
        self.session_maker = sessionmaker(bind=self.engine)
        self.use_sqlalchemy = True
    
    def _create_tables_native_sqlite(self):
        """
        Crea tablas usando SQLite nativo (fallback)
        """
        cursor = self.sqlite_connection.cursor()
        
        # Tabla de transacciones
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                price REAL NOT NULL,
                user_id TEXT NOT NULL,
                source_file TEXT NOT NULL,
                batch_id TEXT,
                bronze_created_at TEXT,
                db_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processing_metadata TEXT
            )
        ''')
        
        # Índices para optimizar consultas
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price ON transactions(price)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_file ON transactions(source_file)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON transactions(batch_id)')
        
        # Tabla de metadatos de batches
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_metadata (
                batch_id TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                batch_number INTEGER NOT NULL,
                rows_processed INTEGER NOT NULL,
                processing_start TIMESTAMP,
                processing_end TIMESTAMP,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                stats_snapshot TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabla de verificaciones
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats_verification (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                verification_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                incremental_stats TEXT NOT NULL,
                database_stats TEXT NOT NULL,
                comparison_result TEXT NOT NULL,
                verification_passed INTEGER NOT NULL,
                notes TEXT
            )
        ''')
        
        self.sqlite_connection.commit()  # ✅ USAR sqlite_connection correctamente
        logger.info("✅ Tablas SQLite nativas creadas/verificadas")
    
    def insert_batch(self, batch_data: pd.DataFrame, batch_info: Dict[str, Any]) -> str:
        """
        Inserta un micro-batch en la base de datos
        ✅ FIXED: Lógica clarificada para SQLAlchemy vs SQLite nativo
        """
        batch_id = str(uuid.uuid4())
        batch_info['batch_id'] = batch_id
        
        try:
            # Registrar inicio del procesamiento
            self._insert_batch_metadata(batch_id, batch_info, status='processing')
            
            # Preparar datos para inserción
            insert_data = batch_data.copy()
            insert_data['batch_id'] = batch_id
            insert_data['db_inserted_at'] = datetime.now()
            
            # ✅ INSERTAR SEGÚN EL MOTOR USADO - FIXED
            if self.use_sqlalchemy:
                # Usar SQLAlchemy - pandas.to_sql funciona mejor con connections
                try:
                    with self.engine.connect() as conn:
                        insert_data.to_sql(
                            'transactions',
                            conn,  # Usar conexión en lugar de engine
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                    logger.debug(f"📝 SQLAlchemy: {len(insert_data)} filas insertadas")
                except Exception as e:
                    logger.warning(f"⚠️ Error con pandas.to_sql: {e}")
                    # Fallback a inserción manual SQLAlchemy
                    self._insert_batch_sqlalchemy_manual(insert_data)
            else:
                # Usar SQLite nativo
                self._insert_batch_native_sqlite(insert_data)
            
            # Actualizar metadata del batch
            self._update_batch_metadata(
                batch_id,
                status='completed',
                processing_end=datetime.now(),
                rows_processed=len(batch_data)
            )
            
            logger.info(f"✅ Batch insertado: {batch_id[:8]}... ({len(batch_data)} filas)")
            return batch_id
            
        except Exception as e:
            logger.error(f"❌ Error insertando batch {batch_id[:8]}...: {e}")
            self._update_batch_metadata(
                batch_id,
                status='failed',
                error_message=str(e),
                processing_end=datetime.now()
            )
            raise
    
    def _insert_batch_sqlalchemy_manual(self, batch_data: pd.DataFrame):
        """
        Inserta batch usando SQLAlchemy manualmente (fallback para pandas.to_sql)
        ✅ SIMPLIFIED: Todo como strings para evitar problemas de tipos
        """
        values_to_insert = []
        
        for _, row in batch_data.iterrows():
            # ✅ SIMPLIFICADO: Convertir todo a string para consistencia
            
            # Procesar db_inserted_at como string ISO
            db_inserted_at = row.get('db_inserted_at')
            if hasattr(db_inserted_at, 'isoformat'):
                db_inserted_at = db_inserted_at.isoformat()
            else:
                db_inserted_at = datetime.now().isoformat()
            
            value_dict = {
                'timestamp': str(row.get('timestamp', '')),
                'price': float(row.get('price', 0.0)),
                'user_id': str(row.get('user_id', '')),
                'source_file': str(row.get('source_file', '')),
                'batch_id': str(row.get('batch_id', '')),
                'bronze_created_at': str(row.get('bronze_created_at', '')),  # String
                'db_inserted_at': db_inserted_at,  # String ISO
                'processing_metadata': None
            }
            values_to_insert.append(value_dict)
        
        # Insertar usando SQLAlchemy
        with self.engine.begin() as conn:
            conn.execute(self.transactions_table.insert(), values_to_insert)
        
        logger.debug(f"📝 SQLAlchemy manual: {len(values_to_insert)} filas insertadas")
    
    def _insert_batch_native_sqlite(self, batch_data: pd.DataFrame):
        """
        Inserta batch usando SQLite nativo - FIXED
        """
        cursor = self.sqlite_connection.cursor()
        
        # Preparar datos para inserción
        columns = ['timestamp', 'price', 'user_id', 'source_file', 'batch_id', 
                  'bronze_created_at', 'db_inserted_at']
        
        values = []
        for _, row in batch_data.iterrows():
            # Convertir timestamp para compatibilidad
            db_inserted_at = row.get('db_inserted_at')
            if hasattr(db_inserted_at, 'isoformat'):
                db_inserted_at = db_inserted_at.isoformat()
            else:
                db_inserted_at = str(db_inserted_at)
            
            value_tuple = (
                str(row.get('timestamp', '')),
                float(row.get('price', 0.0)),
                str(row.get('user_id', '')),
                str(row.get('source_file', '')),
                str(row.get('batch_id', '')),
                str(row.get('bronze_created_at', '')),
                db_inserted_at
            )
            values.append(value_tuple)
        
        # Inserción por lotes
        placeholders = ','.join(['?' * len(columns)])
        query = f"INSERT INTO transactions ({','.join(columns)}) VALUES ({placeholders})"
        
        cursor.executemany(query, values)
        self.sqlite_connection.commit()  # ✅ FIXED: usar sqlite_connection
        
        logger.debug(f"📝 SQLite nativo: {len(values)} filas insertadas")
    
    def _insert_batch_metadata(self, batch_id: str, batch_info: Dict[str, Any], status: str = 'pending'):
        """
        Inserta metadata del batch - SIMPLIFIED
        """
        now_str = datetime.now().isoformat()
        
        metadata = {
            'batch_id': batch_id,
            'source_file': batch_info.get('source_file', ''),
            'batch_number': batch_info.get('batch_number', 0),
            'rows_processed': batch_info.get('rows_processed', 0),
            'processing_start': now_str if status == 'processing' else '',
            'processing_end': '',  # Se actualiza después
            'status': status,
            'stats_snapshot': batch_info.get('stats_snapshot', ''),
            'created_at': now_str
        }
        
        if self.use_sqlalchemy:
            # ✅ USAR SQLAlchemy correctamente
            with self.engine.begin() as conn:
                conn.execute(self.batch_metadata_table.insert().values(**metadata))
        else:
            # ✅ USAR SQLite nativo correctamente
            cursor = self.sqlite_connection.cursor()
            cursor.execute('''
                INSERT INTO batch_metadata 
                (batch_id, source_file, batch_number, rows_processed, processing_start, processing_end, status, stats_snapshot, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metadata['batch_id'], 
                metadata['source_file'], 
                metadata['batch_number'],
                metadata['rows_processed'], 
                metadata['processing_start'],
                metadata['processing_end'], 
                metadata['status'],
                metadata['stats_snapshot'], 
                metadata['created_at']
            ))
            self.sqlite_connection.commit()
    
    def _update_batch_metadata(self, batch_id: str, **updates):
        """
        Actualiza metadata del batch - SIMPLIFIED
        """
        # Convertir datetime a string si es necesario
        for key, value in updates.items():
            if hasattr(value, 'isoformat'):
                updates[key] = value.isoformat()
        
        if self.use_sqlalchemy:
            # ✅ USAR SQLAlchemy correctamente
            with self.engine.begin() as conn:
                conn.execute(
                    self.batch_metadata_table.update()
                    .where(self.batch_metadata_table.c.batch_id == batch_id)
                    .values(**updates)
                )
        else:
            # ✅ USAR SQLite nativo correctamente
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values()) + [batch_id]
            
            cursor = self.sqlite_connection.cursor()
            cursor.execute(f"UPDATE batch_metadata SET {set_clause} WHERE batch_id = ?", values)
            self.sqlite_connection.commit()
    
    def get_database_statistics(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas directas de la base de datos
        ✅ FIXED: Manejo correcto de conexiones
        """
        logger.info("🔍 Calculando estadísticas directas desde base de datos...")
        
        try:
            query = """
                SELECT 
                    COUNT(*) as count,
                    AVG(price) as avg,
                    MIN(price) as min,
                    MAX(price) as max,
                    SUM(price) as sum
                FROM transactions
            """
            
            if self.use_sqlalchemy:
                # ✅ USAR SQLAlchemy correctamente
                with self.engine.connect() as conn:
                    result = conn.execute(sa.text(query)).fetchone()
            else:
                # ✅ USAR SQLite nativo correctamente
                cursor = self.sqlite_connection.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
            
            if result and result[0] > 0:
                stats = {
                    'count': result[0],
                    'avg': float(result[1]) if result[1] is not None else 0.0,
                    'min': float(result[2]) if result[2] is not None else 0.0,
                    'max': float(result[3]) if result[3] is not None else 0.0,
                    'sum': float(result[4]) if result[4] is not None else 0.0,
                    'calculated_at': datetime.now().isoformat(),
                    'source': 'direct_database_query'
                }
                
                logger.info(f"📊 Estadísticas BD: Count={stats['count']:,}, Avg=${stats['avg']:.2f}")
                return stats
            else:
                logger.info("📊 Base de datos vacía")
                return {
                    'count': 0, 'avg': 0.0, 'min': 0.0, 'max': 0.0, 'sum': 0.0,
                    'calculated_at': datetime.now().isoformat(),
                    'source': 'direct_database_query'
                }
                
        except Exception as e:
            logger.error(f"❌ Error calculando estadísticas de BD: {e}")
            raise
    
    def save_verification_result(self, incremental_stats: Dict, db_stats: Dict, comparison_result: Dict):
        """
        Guarda resultado de verificación en BD para auditoría - SIMPLIFIED
        """
        import json
        
        verification_data = {
            'verification_timestamp': datetime.now().isoformat(),  # String ISO
            'incremental_stats': json.dumps(incremental_stats),
            'database_stats': json.dumps(db_stats),
            'comparison_result': json.dumps(comparison_result),
            'verification_passed': 1 if comparison_result.get('overall_match', False) else 0,
            'notes': f"Verification at {datetime.now().isoformat()}"
        }
        
        try:
            if self.use_sqlalchemy:
                # ✅ USAR SQLAlchemy correctamente
                with self.engine.begin() as conn:
                    conn.execute(self.stats_verification_table.insert().values(**verification_data))
            else:
                # ✅ USAR SQLite nativo correctamente
                cursor = self.sqlite_connection.cursor()
                cursor.execute('''
                    INSERT INTO stats_verification 
                    (verification_timestamp, incremental_stats, database_stats, comparison_result, verification_passed, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    verification_data['verification_timestamp'],
                    verification_data['incremental_stats'],
                    verification_data['database_stats'],
                    verification_data['comparison_result'],
                    verification_data['verification_passed'],
                    verification_data['notes']
                ))
                self.sqlite_connection.commit()
            
            logger.info("💾 Resultado de verificación guardado en BD")
            
        except Exception as e:
            logger.error(f"❌ Error guardando verificación: {e}")
    
    def get_batch_summary(self) -> Dict[str, Any]:
        """
        Obtiene resumen de batches procesados - FIXED
        """
        try:
            if self.use_sqlalchemy:
                query = """
                    SELECT 
                        COUNT(*) as total_batches,
                        SUM(rows_processed) as total_rows,
                        AVG(rows_processed) as avg_batch_size,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_batches,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_batches
                    FROM batch_metadata
                """
                with self.engine.connect() as conn:
                    result = conn.execute(sa.text(query)).fetchone()
            else:
                # SQLite nativo - ajustar query
                sqlite_query = """
                    SELECT 
                        COUNT(*) as total_batches,
                        SUM(rows_processed) as total_rows,
                        AVG(rows_processed) as avg_batch_size,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_batches,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_batches
                    FROM batch_metadata
                """
                cursor = self.sqlite_connection.cursor()
                cursor.execute(sqlite_query)
                result = cursor.fetchone()
            
            if result:
                return {
                    'total_batches': result[0],
                    'total_rows': result[1] or 0,
                    'avg_batch_size': float(result[2]) if result[2] else 0.0,
                    'completed_batches': result[3],
                    'failed_batches': result[4],
                    'success_rate': (result[3] / result[0] * 100) if result[0] > 0 else 0.0
                }
            else:
                return {
                    'total_batches': 0, 'total_rows': 0, 'avg_batch_size': 0.0, 
                    'completed_batches': 0, 'failed_batches': 0, 'success_rate': 0.0
                }
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo resumen de batches: {e}")
            return {}
    
    def close(self):
        """
        Cierra conexiones a la base de datos - FIXED
        """
        try:
            if self.engine:
                self.engine.dispose()
            if self.sqlite_connection:
                self.sqlite_connection.close()
            logger.info("🔒 Conexiones de BD cerradas")
        except Exception as e:
            logger.error(f"❌ Error cerrando BD: {e}")


def create_database_manager(config: Optional[Dict[str, Any]] = None) -> DatabaseManager:
    """
    Factory function para crear DatabaseManager con configuración
    """
    return DatabaseManager(config)