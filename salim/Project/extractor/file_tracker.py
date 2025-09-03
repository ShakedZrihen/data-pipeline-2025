import psycopg2
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class FileTracker:
    """Manages tracking of processed files to prevent duplicates and enable resume functionality"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.connection = None
        self.connect()
    
    def connect(self):
        """Connect to the database"""
        try:
            self.connection = psycopg2.connect(self.database_url)
            logger.info("File tracker connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def register_file(self, s3_key: str, file_type: str, supermarket: str, 
                     file_size: int = None, s3_last_modified: datetime = None) -> bool:
        """Register a new file for processing"""
        try:
            cursor = self.connection.cursor()
            
            # Insert or update file record
            query = """
            INSERT INTO processed_files (
                s3_key, file_type, supermarket, file_size, s3_last_modified,
                processing_status, created_at
            ) VALUES (%s, %s, %s, %s, %s, 'pending', %s)
            ON CONFLICT (s3_key) 
            DO UPDATE SET
                file_size = EXCLUDED.file_size,
                s3_last_modified = EXCLUDED.s3_last_modified,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id, processing_status
            """
            
            cursor.execute(query, (
                s3_key, file_type, supermarket, file_size, 
                s3_last_modified, datetime.now()
            ))
            
            result = cursor.fetchone()
            self.connection.commit()
            cursor.close()
            
            if result:
                file_id, status = result
                logger.info(f"Registered file {s3_key} with status: {status}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to register file {s3_key}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def is_file_processed(self, s3_key: str) -> bool:
        """Check if a file has already been successfully processed"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            SELECT processing_status, s3_last_modified 
            FROM processed_files 
            WHERE s3_key = %s
            """
            
            cursor.execute(query, (s3_key,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                status, _ = result
                return status == 'completed'
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check file status for {s3_key}: {e}")
            return False
    
    def should_process_file(self, s3_key: str, s3_last_modified: datetime) -> bool:
        """Determine if a file should be processed based on modification time"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            SELECT processing_status, s3_last_modified, retry_count
            FROM processed_files 
            WHERE s3_key = %s
            """
            
            cursor.execute(query, (s3_key,))
            result = cursor.fetchone()
            cursor.close()
            
            if not result:
                # New file, should process
                return True
            
            status, stored_modified, retry_count = result
            
            # If file is completed and hasn't been modified, skip
            if status == 'completed' and stored_modified and stored_modified >= s3_last_modified:
                return False
            
            # If file failed but retry count is too high, skip
            if status == 'failed' and retry_count >= 3:
                logger.warning(f"Skipping file {s3_key} - max retries exceeded")
                return False
            
            # Process if file is newer or failed with retries available
            return True
            
        except Exception as e:
            logger.error(f"Failed to check processing status for {s3_key}: {e}")
            return True  # Process by default on error
    
    def mark_processing_started(self, s3_key: str) -> bool:
        """Mark file as currently being processed"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            UPDATE processed_files 
            SET processing_status = 'processing',
                processing_started_at = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE s3_key = %s
            """
            
            cursor.execute(query, (datetime.now(), s3_key))
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Marked file {s3_key} as processing")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark file as processing {s3_key}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def mark_processing_completed(self, s3_key: str) -> bool:
        """Mark file as successfully processed"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            UPDATE processed_files 
            SET processing_status = 'completed',
                processing_completed_at = %s,
                error_message = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE s3_key = %s
            """
            
            cursor.execute(query, (datetime.now(), s3_key))
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Marked file {s3_key} as completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark file as completed {s3_key}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def mark_processing_failed(self, s3_key: str, error_message: str) -> bool:
        """Mark file as failed with error message"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            UPDATE processed_files 
            SET processing_status = 'failed',
                error_message = %s,
                retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE s3_key = %s
            """
            
            cursor.execute(query, (error_message, s3_key))
            self.connection.commit()
            cursor.close()
            
            logger.warning(f"Marked file {s3_key} as failed: {error_message}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark file as failed {s3_key}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def get_pending_files(self, supermarket: str = None, file_type: str = None) -> List[Dict[str, Any]]:
        """Get list of files pending processing"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            SELECT s3_key, file_type, supermarket, file_size, s3_last_modified, retry_count
            FROM processed_files 
            WHERE processing_status IN ('pending', 'failed') 
            AND retry_count < 3
            """
            params = []
            
            if supermarket:
                query += " AND supermarket = %s"
                params.append(supermarket)
            
            if file_type:
                query += " AND file_type = %s"
                params.append(file_type)
            
            query += " ORDER BY s3_last_modified DESC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            files = []
            for row in results:
                files.append({
                    's3_key': row[0],
                    'file_type': row[1],
                    'supermarket': row[2],
                    'file_size': row[3],
                    's3_last_modified': row[4],
                    'retry_count': row[5]
                })
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to get pending files: {e}")
            return []
    
    def get_processing_stats(self) -> Dict[str, int]:
        """Get processing statistics"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            SELECT 
                processing_status,
                COUNT(*) as count
            FROM processed_files 
            GROUP BY processing_status
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            stats = {}
            for row in results:
                stats[row[0]] = row[1]
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
            return {}
    
    def cleanup_old_records(self, days_old: int = 30) -> int:
        """Remove old completed records to keep table size manageable"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            DELETE FROM processed_files 
            WHERE processing_status = 'completed' 
            AND processing_completed_at < NOW() - INTERVAL '%s days'
            """
            
            cursor.execute(query, (days_old,))
            deleted_count = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Cleaned up {deleted_count} old processed file records")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old records: {e}")
            if self.connection:
                self.connection.rollback()
            return 0
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("File tracker database connection closed")
