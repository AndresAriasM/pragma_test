#src/data_flow/download_data.py
"""
Script para descargar datos desde Google Drive

"""

import os
import logging
import requests
import zipfile
from pathlib import Path
from typing import Optional, Tuple
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataDownloader:
    """
    Clase para manejar la descarga de datos desde Google Drive
    """
    
    def __init__(self, base_path: str = None):
        """
        Initialize the DataDownloader
        
        Args:
            base_path: Ruta base del proyecto. Si no se proporciona, usa el directorio actual
        """
        if base_path is None:
            base_path = Path(__file__).parent.parent.parent
        
        self.base_path = Path(base_path)
        self.raw_data_path = self.base_path / "data" / "raw"
        self.processed_data_path = self.base_path / "data" / "processed"
        
        # Crear directorios si no existen
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"DataDownloader inicializado. Ruta base: {self.base_path}")
        logger.info(f"Datos raw: {self.raw_data_path}")
    
    def download_from_google_drive(
        self, 
        file_id: str, 
        destination: str,
        chunk_size: int = 32768
    ) -> bool:
        """
        Descarga un archivo desde Google Drive
        
        Args:
            file_id: ID del archivo en Google Drive
            destination: Ruta de destino para guardar el archivo
            chunk_size: Tamaño del chunk para la descarga
            
        Returns:
            bool: True si la descarga fue exitosa, False en caso contrario
        """
        try:
            logger.info(f"Iniciando descarga del archivo {file_id}")
            
            # URL de descarga directa de Google Drive
            url = f"https://drive.google.com/uc?id={file_id}&export=download"
            
            # Realizar la solicitud
            session = requests.Session()
            response = session.get(url, stream=True)
            
            # Verificar si necesitamos confirmar la descarga (archivos grandes)
            if 'download_warning' in response.headers.get('Set-Cookie', ''):
                params = {'id': file_id, 'confirm': 't'}
                response = session.get(url, params=params, stream=True)
            
            response.raise_for_status()
            
            # Obtener el tamaño total si está disponible
            total_size = int(response.headers.get('content-length', 0))
            
            # Descargar el archivo
            with open(destination, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Mostrar progreso cada MB
                        if downloaded % (1024 * 1024) == 0:
                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                logger.info(f"Descargado: {downloaded // (1024*1024)} MB ({progress:.1f}%)")
                            else:
                                logger.info(f"Descargado: {downloaded // (1024*1024)} MB")
            
            logger.info(f"✅ Descarga completada: {destination}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error de red durante la descarga: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error inesperado durante la descarga: {e}")
            return False
    
    def extract_zip_file(self, zip_path: str, extract_to: str = None) -> bool:
        """
        Extrae un archivo ZIP
        
        Args:
            zip_path: Ruta del archivo ZIP
            extract_to: Directorio de extracción (por defecto: mismo directorio que el ZIP)
            
        Returns:
            bool: True si la extracción fue exitosa, False en caso contrario
        """
        try:
            if extract_to is None:
                extract_to = Path(zip_path).parent
            
            logger.info(f"Extrayendo {zip_path} en {extract_to}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Listar archivos en el ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Archivos en el ZIP: {file_list}")
                
                # Extraer todos los archivos
                zip_ref.extractall(extract_to)
                
                logger.info(f"✅ Extracción completada en: {extract_to}")
                
                # Verificar archivos extraídos
                for file_name in file_list:
                    extracted_file = Path(extract_to) / file_name
                    if extracted_file.exists():
                        file_size = extracted_file.stat().st_size
                        logger.info(f"  📄 {file_name}: {file_size:,} bytes")
                    else:
                        logger.warning(f"  ⚠️ No se encontró: {file_name}")
                
                return True
                
        except zipfile.BadZipFile:
            logger.error(f"❌ Archivo ZIP corrupto: {zip_path}")
            return False
        except Exception as e:
            logger.error(f"❌ Error durante la extracción: {e}")
            return False
    
    def download_challenge_data(self, force_download: bool = False) -> Tuple[bool, Optional[str]]:
        """
        Descarga los datos específicos del challenge desde Google Drive
        VERSIÓN NO INTERACTIVA - Compatible con Airflow
        
        Args:
            force_download: Si True, descarga aunque el archivo ya exista
        
        Returns:
            Tuple[bool, Optional[str]]: (éxito, ruta del archivo descargado)
        """
        # ID extraído del enlace proporcionado
        google_drive_file_id = "1ejZpGTvZa81ZGD7IRWjObFeVuYbsSvuB"
        
        # Ruta de destino
        zip_filename = "challenge_data.zip"
        zip_path = self.raw_data_path / zip_filename
        
        logger.info("🚀 Iniciando descarga de datos del challenge")
        logger.info(f"Google Drive File ID: {google_drive_file_id}")
        logger.info(f"Destino: {zip_path}")
        
        # ✅ LÓGICA NO INTERACTIVA
        if zip_path.exists() and not force_download:
            logger.info(f"ℹ️ El archivo ya existe: {zip_path}")
            logger.info("ℹ️ Saltando descarga, usando archivo existente")
            
            # Verificar si ya está extraído
            extracted_folder = self.find_extracted_folder()
            if extracted_folder:
                logger.info(f"✅ Datos ya descargados y extraídos en: {extracted_folder}")
                return True, str(zip_path)
            else:
                logger.info("🔄 Archivo existe pero no está extraído, extrayendo...")
                extraction_success = self.extract_zip_file(str(zip_path))
                if extraction_success:
                    logger.info("🎉 Extracción completada exitosamente")
                    return True, str(zip_path)
                else:
                    logger.error("❌ Error durante la extracción")
                    return False, None
        
        # Descargar el archivo
        logger.info("📥 Iniciando descarga desde Google Drive...")
        success = self.download_from_google_drive(
            file_id=google_drive_file_id,
            destination=str(zip_path)
        )
        
        if success:
            # Verificar el tamaño del archivo descargado
            file_size = zip_path.stat().st_size
            logger.info(f"📁 Archivo descargado: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
            
            # Extraer el archivo ZIP
            extraction_success = self.extract_zip_file(str(zip_path))
            
            if extraction_success:
                logger.info("🎉 Descarga y extracción completadas exitosamente")
                return True, str(zip_path)
            else:
                logger.error("❌ Error durante la extracción")
                return False, None
        else:
            logger.error("❌ Error durante la descarga")
            return False, None
    
    def find_extracted_folder(self) -> Optional[Path]:
        """
        Busca la carpeta extraída que contiene los archivos CSV
        
        Returns:
            Path de la carpeta extraída o None si no se encuentra
        """
        # Buscar carpetas en raw_data_path
        for item in self.raw_data_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Verificar si contiene archivos CSV
                csv_files = list(item.glob("*.csv"))
                if len(csv_files) >= 5:  # Al menos 5 archivos CSV
                    logger.info(f"📁 Carpeta extraída encontrada: {item.name}")
                    return item
        
        # Buscar directamente en raw_data_path
        csv_files = list(self.raw_data_path.glob("*.csv"))
        if len(csv_files) >= 5:
            logger.info(f"📁 Archivos CSV encontrados directamente en raw")
            return self.raw_data_path
        
        logger.warning("❌ No se encontró carpeta extraída con archivos CSV")
        return None

    def verify_downloaded_data(self) -> bool:
        """
        Verifica que los archivos CSV esperados estén presentes en la carpeta extraída
        
        Returns:
            bool: True si todos los archivos están presentes
        """
        expected_files = [
            "2012-1.csv",
            "2012-2.csv", 
            "2012-3.csv",
            "2012-4.csv",
            "2012-5.csv",
            "validation.csv"
        ]
        
        logger.info("🔍 Verificando archivos descargados...")
        
        # Buscar la carpeta extraída
        extracted_folder = self.find_extracted_folder()
        if not extracted_folder:
            logger.error("❌ No se encontró la carpeta extraída")
            return False
        
        logger.info(f"📂 Verificando archivos en: {extracted_folder.name}")
        
        all_files_present = True
        for file_name in expected_files:
            file_path = extracted_folder / file_name
            if file_path.exists():
                file_size = file_path.stat().st_size
                logger.info(f"  ✅ {file_name}: {file_size:,} bytes")
            else:
                logger.warning(f"  ❌ Falta: {file_name}")
                all_files_present = False
        
        if all_files_present:
            logger.info("🎉 Todos los archivos están presentes")
        else:
            logger.warning("⚠️ Algunos archivos están faltando")
        
        return all_files_present
    
    def cleanup_zip_file(self, zip_path: str) -> bool:
        """
        Limpia el archivo ZIP después de la extracción
        
        Args:
            zip_path: Ruta del archivo ZIP a eliminar
            
        Returns:
            bool: True si la limpieza fue exitosa
        """
        try:
            if Path(zip_path).exists():
                os.remove(zip_path)
                logger.info(f"🗑️ Archivo ZIP eliminado: {zip_path}")
                return True
            return True
        except Exception as e:
            logger.error(f"❌ Error eliminando ZIP: {e}")
            return False


def download_challenge_data_from_drive(force_download: bool = False) -> dict:
    """
    Función de conveniencia para descargar datos desde Google Drive
    ✅ NO INTERACTIVA - Para uso en Airflow
    
    Args:
        force_download: Si True, fuerza descarga aunque el archivo exista
        
    Returns:
        dict: Resultado de la descarga
    """
    try:
        downloader = DataDownloader()
        success, zip_path = downloader.download_challenge_data(force_download=force_download)
        
        if success:
            verification_success = downloader.verify_downloaded_data()
            
            if verification_success:
                return {
                    'success': True,
                    'zip_path': zip_path,
                    'files_downloaded': 6,
                    'extraction_path': str(downloader.find_extracted_folder()),
                    'message': 'Download and verification successful'
                }
            else:
                return {
                    'success': False,
                    'error': 'File verification failed',
                    'zip_path': zip_path
                }
        else:
            return {
                'success': False,
                'error': 'Download failed',
                'zip_path': None
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'zip_path': None
        }


def main():
    """
    Función principal para ejecutar la descarga
    ✅ VERSIÓN NO INTERACTIVA
    """
    try:
        logger.info("🚀 Iniciando descarga no interactiva")
        
        # Inicializar el descargador
        downloader = DataDownloader()
        
        # Descargar datos del challenge (no interactivo)
        success, zip_path = downloader.download_challenge_data(force_download=False)
        
        if success:
            # Verificar archivos
            if downloader.verify_downloaded_data():
                logger.info("✅ Proceso de descarga completado exitosamente")
                return 0
            else:
                logger.error("❌ Faltan algunos archivos después de la descarga")
                return 1
        else:
            logger.error("❌ Error durante la descarga")
            return 1
            
    except KeyboardInterrupt:
        logger.info("❌ Descarga cancelada por el usuario")
        return 1
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)