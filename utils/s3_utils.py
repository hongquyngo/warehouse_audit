# utils/s3_utils.py - S3 Utilities for Warehouse Audit System

import boto3
from botocore.exceptions import ClientError
import logging
import json
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import os
from .config import config

# Setup logger
logger = logging.getLogger(__name__)

class S3Manager:
    """S3 Manager for Warehouse Audit System"""
    
    def __init__(self):
        """Initialize S3 client with credentials from config"""
        try:
            # Get AWS config
            aws_config = config.aws_config
            
            # Validate required config
            if not all([
                aws_config.get('access_key_id'),
                aws_config.get('secret_access_key'),
                aws_config.get('region'),
                aws_config.get('bucket_name')
            ]):
                raise ValueError("Missing required AWS configuration")
            
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_config['access_key_id'],
                aws_secret_access_key=aws_config['secret_access_key'],
                region_name=aws_config['region']
            )
            
            self.bucket_name = aws_config['bucket_name']
            self.app_prefix = 'streamlit-app/warehouse-audit'
            
            logger.info(f"✅ S3Manager initialized for bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3Manager: {e}")
            raise
    
    # ==================== Basic S3 Operations ====================
    
    def upload_file(self, file_content: bytes, key: str, content_type: str = None) -> Tuple[bool, str]:
        """
        Upload file to S3
        
        Args:
            file_content: File content as bytes
            key: S3 key (path) for the file
            content_type: MIME type of the file
            
        Returns:
            Tuple of (success: bool, s3_key_or_error: str)
        """
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=file_content,
                **extra_args
            )
            
            logger.info(f"Successfully uploaded file to: {key}")
            return True, key
            
        except ClientError as e:
            error_msg = f"Failed to upload file: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def download_file(self, key: str) -> Optional[bytes]:
        """
        Download file content from S3
        
        Args:
            key: S3 key of the file
            
        Returns:
            File content as bytes or None if error
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            content = response['Body'].read()
            logger.info(f"Successfully downloaded file: {key}")
            return content
            
        except ClientError as e:
            logger.error(f"Error downloading file {key}: {e}")
            return None
    
    def delete_file(self, key: str) -> bool:
        """
        Delete file from S3
        
        Args:
            key: S3 key of the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            logger.info(f"Successfully deleted file: {key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting file {key}: {e}")
            return False
    
    def list_files(self, prefix: str = '', max_keys: int = 1000) -> List[Dict]:
        """
        List files in S3 bucket with optional prefix filter
        
        Args:
            prefix: S3 prefix to filter files
            max_keys: Maximum number of files to return
            
        Returns:
            List of file dictionaries with metadata
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip directory markers
                    if obj['Key'].endswith('/'):
                        continue
                        
                    files.append({
                        'key': obj['Key'],
                        'name': obj['Key'].split('/')[-1],
                        'size': obj['Size'],
                        'size_mb': round(obj['Size'] / 1024 / 1024, 2),
                        'last_modified': obj['LastModified'],
                        'etag': obj.get('ETag', '').strip('"')
                    })
            
            logger.info(f"Listed {len(files)} files with prefix: {prefix}")
            return files
            
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def get_presigned_url(self, key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate presigned URL for file access
        
        Args:
            key: S3 key of the file
            expiration: URL expiration time in seconds (default 1 hour)
            
        Returns:
            Presigned URL or None if error
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': key
                },
                ExpiresIn=expiration
            )
            return url
            
        except ClientError as e:
            logger.error(f"Error generating presigned URL for {key}: {e}")
            return None
    
    def file_exists(self, key: str) -> bool:
        """
        Check if file exists in S3
        
        Args:
            key: S3 key to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return True
        except ClientError:
            return False
    
    # ==================== Warehouse Audit Specific Methods ====================
    
    def upload_audit_attachment(self, file_content: bytes, filename: str, 
                              entity_type: str, entity_code: str, 
                              entity_id: int = None, file_category: str = 'docs',
                              content_type: str = None) -> Tuple[bool, str]:
        """
        Upload attachment for audit entities
        
        Args:
            file_content: File content as bytes
            filename: Original filename
            entity_type: Type of entity ('session', 'transaction', 'count_detail')
            entity_code: Code of entity (session_code, transaction_code)
            entity_id: ID for count_detail
            file_category: 'docs' or 'images'
            content_type: MIME type of the file
        
        Returns:
            Tuple of (success: bool, s3_key_or_error: str)
        """
        # Validate inputs
        valid_entity_types = ['session', 'transaction', 'count_detail']
        if entity_type not in valid_entity_types:
            return False, f"Invalid entity type. Must be one of: {valid_entity_types}"
        
        valid_categories = ['docs', 'images']
        if file_category not in valid_categories:
            return False, f"Invalid file category. Must be one of: {valid_categories}"
        
        if entity_type == 'count_detail' and entity_id is None:
            return False, "entity_id is required for count_detail"
        
        # Generate safe filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = "".join(c if c.isalnum() or c in ".-_" else "_" for c in filename)
        
        # Build S3 key based on entity type
        if entity_type == 'session':
            key = f"{self.app_prefix}/sessions/{entity_code}/session-{file_category}/{timestamp}_{safe_filename}"
        elif entity_type == 'transaction':
            key = f"{self.app_prefix}/transactions/{entity_code}/transaction-{file_category}/{timestamp}_{safe_filename}"
        elif entity_type == 'count_detail':
            key = f"{self.app_prefix}/count-details/{entity_code}/{entity_id}/count-{file_category}/{timestamp}_{safe_filename}"
        
        return self.upload_file(file_content, key, content_type)
    
    def list_audit_attachments(self, entity_type: str, entity_code: str, 
                             entity_id: int = None, file_category: str = None) -> List[Dict]:
        """
        List attachments for an audit entity
        
        Args:
            entity_type: Type of entity ('session', 'transaction', 'count_detail')
            entity_code: Code of entity (session_code, transaction_code)
            entity_id: ID for count_detail (required if entity_type is 'count_detail')
            file_category: Optional filter by 'docs' or 'images'
            
        Returns:
            List of file dictionaries
        """
        # Build prefix based on entity type
        if entity_type == 'session':
            base_prefix = f"{self.app_prefix}/sessions/{entity_code}/"
            if file_category:
                prefix = f"{base_prefix}session-{file_category}/"
            else:
                prefix = base_prefix
                
        elif entity_type == 'transaction':
            base_prefix = f"{self.app_prefix}/transactions/{entity_code}/"
            if file_category:
                prefix = f"{base_prefix}transaction-{file_category}/"
            else:
                prefix = base_prefix
                
        elif entity_type == 'count_detail' and entity_id is not None:
            base_prefix = f"{self.app_prefix}/count-details/{entity_code}/{entity_id}/"
            if file_category:
                prefix = f"{base_prefix}count-{file_category}/"
            else:
                prefix = base_prefix
        else:
            logger.error(f"Invalid parameters for listing attachments: {entity_type}, {entity_id}")
            return []
        
        files = self.list_files(prefix=prefix)
        
        # Add additional metadata to help with display
        for file in files:
            # Determine file category from path
            if '/session-docs/' in file['key'] or '/transaction-docs/' in file['key'] or '/count-docs/' in file['key']:
                file['category'] = 'docs'
            elif '/session-images/' in file['key'] or '/transaction-images/' in file['key'] or '/count-images/' in file['key']:
                file['category'] = 'images'
            else:
                file['category'] = 'unknown'
            
            # Extract original filename (remove timestamp prefix)
            name_parts = file['name'].split('_', 2)
            if len(name_parts) > 2:
                file['original_name'] = name_parts[2]
            else:
                file['original_name'] = file['name']
        
        return files
    
    def create_audit_folders(self):
        """Create initial folder structure for warehouse audit"""
        folders = [
            f'{self.app_prefix}/',
            f'{self.app_prefix}/sessions/',
            f'{self.app_prefix}/transactions/',
            f'{self.app_prefix}/count-details/',
        ]
        
        created_count = 0
        for folder in folders:
            try:
                # Check if folder marker already exists
                if not self.file_exists(folder):
                    # Create folder by creating an empty object with trailing slash
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=folder,
                        Body=b''
                    )
                    logger.info(f"Created folder: {folder}")
                    created_count += 1
                else:
                    logger.info(f"Folder already exists: {folder}")
                    
            except Exception as e:
                logger.error(f"Error creating folder {folder}: {e}")
        
        logger.info(f"Audit folder setup complete. Created {created_count} new folders.")
        return created_count
    
    def batch_delete(self, keys: List[str]) -> Dict[str, List[str]]:
        """
        Delete multiple files at once
        
        Args:
            keys: List of S3 keys to delete
            
        Returns:
            Dictionary with 'deleted' and 'errors' lists
        """
        result = {'deleted': [], 'errors': []}
        
        if not keys:
            return result
        
        try:
            # S3 batch delete accepts max 1000 keys at once
            for i in range(0, len(keys), 1000):
                batch = keys[i:i+1000]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': [{'Key': key} for key in batch],
                        'Quiet': False
                    }
                )
                
                if 'Deleted' in response:
                    result['deleted'].extend([obj['Key'] for obj in response['Deleted']])
                
                if 'Errors' in response:
                    result['errors'].extend([
                        f"{err['Key']}: {err['Message']}" 
                        for err in response['Errors']
                    ])
            
            logger.info(f"Batch delete complete. Deleted: {len(result['deleted'])}, Errors: {len(result['errors'])}")
            
        except ClientError as e:
            logger.error(f"Error in batch delete: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def get_file_info(self, key: str) -> Optional[Dict]:
        """
        Get detailed file information
        
        Args:
            key: S3 key of the file
            
        Returns:
            Dictionary with file metadata or None if error
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            return {
                'key': key,
                'size': response['ContentLength'],
                'size_mb': round(response['ContentLength'] / 1024 / 1024, 2),
                'content_type': response.get('ContentType', 'unknown'),
                'last_modified': response['LastModified'],
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {})
            }
            
        except ClientError as e:
            logger.error(f"Error getting file info for {key}: {e}")
            return None
    
    def copy_file(self, source_key: str, dest_key: str) -> bool:
        """
        Copy file within S3
        
        Args:
            source_key: Source S3 key
            dest_key: Destination S3 key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            
            logger.info(f"Successfully copied {source_key} to {dest_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error copying file: {e}")
            return False
    
    def move_attachment(self, old_key: str, new_entity_type: str, 
                       new_entity_code: str, new_entity_id: int = None) -> Tuple[bool, str]:
        """
        Move attachment to a new entity (copy and delete)
        
        Args:
            old_key: Current S3 key
            new_entity_type: New entity type
            new_entity_code: New entity code
            new_entity_id: New entity ID (for count_detail)
            
        Returns:
            Tuple of (success: bool, new_key_or_error: str)
        """
        try:
            # Extract filename and category from old key
            filename = old_key.split('/')[-1]
            
            # Determine file category
            if '/docs/' in old_key or '-docs/' in old_key:
                file_category = 'docs'
            elif '/images/' in old_key or '-images/' in old_key:
                file_category = 'images'
            else:
                file_category = 'docs'  # default
            
            # Build new key
            if new_entity_type == 'session':
                new_key = f"{self.app_prefix}/sessions/{new_entity_code}/session-{file_category}/{filename}"
            elif new_entity_type == 'transaction':
                new_key = f"{self.app_prefix}/transactions/{new_entity_code}/transaction-{file_category}/{filename}"
            elif new_entity_type == 'count_detail' and new_entity_id:
                new_key = f"{self.app_prefix}/count-details/{new_entity_code}/{new_entity_id}/count-{file_category}/{filename}"
            else:
                return False, "Invalid parameters for move operation"
            
            # Copy to new location
            if self.copy_file(old_key, new_key):
                # Delete from old location
                if self.delete_file(old_key):
                    return True, new_key
                else:
                    # Try to clean up the copy if delete failed
                    self.delete_file(new_key)
                    return False, "Failed to delete original file"
            else:
                return False, "Failed to copy file"
                
        except Exception as e:
            error_msg = f"Error moving attachment: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def generate_attachment_url(self, key: str, expiration: int = 3600, 
                               download: bool = False, filename: str = None) -> Optional[str]:
        """
        Generate presigned URL with optional download headers
        
        Args:
            key: S3 key of the file
            expiration: URL expiration in seconds
            download: If True, force download instead of inline view
            filename: Optional filename for download
            
        Returns:
            Presigned URL or None
        """
        try:
            params = {
                'Bucket': self.bucket_name,
                'Key': key
            }
            
            if download and filename:
                params['ResponseContentDisposition'] = f'attachment; filename="{filename}"'
            
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params=params,
                ExpiresIn=expiration
            )
            return url
            
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {e}")
            return None