"""MinIO client wrapper for S3-compatible operations.

This module provides an async wrapper around aioboto3 for MinIO operations.
"""

import asyncio
from typing import Optional, Dict, Any
import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
import structlog

logger = structlog.get_logger(__name__)


class MinIOClient:
    """Async MinIO client using aioboto3."""

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        max_pool_connections: int = 50,
    ):
        """
        Initialize MinIO client.

        Args:
            endpoint_url: MinIO endpoint (e.g., http://minio:9000)
            access_key: Access key ID
            secret_key: Secret access key
            region: AWS region (default: us-east-1)
            max_pool_connections: Maximum connection pool size
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

        self.config = Config(
            max_pool_connections=max_pool_connections,
            retries={
                'max_attempts': 3,
                'mode': 'adaptive'
            }
        )

        self.session = aioboto3.Session()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    def get_client(self):
        """
        Get an async S3 client context manager.

        Usage:
            async with minio_client.get_client() as s3:
                await s3.put_object(...)
        """
        return self.session.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=self.config
        )

    async def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a bucket if it doesn't exist.

        Args:
            bucket_name: Name of the bucket

        Returns:
            True if bucket was created, False if it already existed
        """
        try:
            async with self.get_client() as s3:
                await s3.create_bucket(Bucket=bucket_name)
                logger.info("bucket_created", bucket=bucket_name)
                return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'BucketAlreadyOwnedByYou':
                logger.debug("bucket_already_exists", bucket=bucket_name)
                return False
            logger.error("bucket_creation_failed", bucket=bucket_name, error=str(e))
            raise

    async def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.

        Args:
            bucket_name: Name of the bucket

        Returns:
            True if bucket exists
        """
        try:
            async with self.get_client() as s3:
                await s3.head_bucket(Bucket=bucket_name)
                return True
        except ClientError:
            return False

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Put an object to MinIO.

        Args:
            bucket: Bucket name
            key: Object key (path)
            data: Object data (bytes)
            metadata: Optional object metadata

        Returns:
            Response from S3 PutObject
        """
        try:
            async with self.get_client() as s3:
                response = await s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data,
                    Metadata=metadata or {}
                )
                logger.debug(
                    "object_uploaded",
                    bucket=bucket,
                    key=key,
                    size_bytes=len(data)
                )
                return response
        except ClientError as e:
            logger.error(
                "object_upload_failed",
                bucket=bucket,
                key=key,
                error=str(e)
            )
            raise

    async def get_object(self, bucket: str, key: str) -> bytes:
        """
        Get an object from MinIO.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            Object data as bytes
        """
        try:
            async with self.get_client() as s3:
                response = await s3.get_object(Bucket=bucket, Key=key)
                data = await response['Body'].read()
                logger.debug(
                    "object_downloaded",
                    bucket=bucket,
                    key=key,
                    size_bytes=len(data)
                )
                return data
        except ClientError as e:
            logger.error(
                "object_download_failed",
                bucket=bucket,
                key=key,
                error=str(e)
            )
            raise

    async def delete_object(self, bucket: str, key: str) -> None:
        """
        Delete an object from MinIO.

        Args:
            bucket: Bucket name
            key: Object key
        """
        try:
            async with self.get_client() as s3:
                await s3.delete_object(Bucket=bucket, Key=key)
                logger.debug("object_deleted", bucket=bucket, key=key)
        except ClientError as e:
            logger.error(
                "object_deletion_failed",
                bucket=bucket,
                key=key,
                error=str(e)
            )
            raise

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000
    ) -> list:
        """
        List objects in a bucket.

        Args:
            bucket: Bucket name
            prefix: Key prefix filter
            max_keys: Maximum number of keys to return

        Returns:
            List of object keys
        """
        try:
            async with self.get_client() as s3:
                response = await s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=max_keys
                )
                objects = response.get('Contents', [])
                keys = [obj['Key'] for obj in objects]
                logger.debug(
                    "objects_listed",
                    bucket=bucket,
                    prefix=prefix,
                    count=len(keys)
                )
                return keys
        except ClientError as e:
            logger.error(
                "object_listing_failed",
                bucket=bucket,
                prefix=prefix,
                error=str(e)
            )
            raise

    async def get_storage_stats(self, bucket: str) -> Dict[str, Any]:
        """
        Get storage statistics for a bucket.

        Args:
            bucket: Bucket name

        Returns:
            Dictionary with object count and total size
        """
        try:
            async with self.get_client() as s3:
                paginator = s3.get_paginator('list_objects_v2')
                total_size = 0
                total_count = 0

                async for page in paginator.paginate(Bucket=bucket):
                    objects = page.get('Contents', [])
                    total_count += len(objects)
                    total_size += sum(obj['Size'] for obj in objects)

                stats = {
                    "object_count": total_count,
                    "total_size_bytes": total_size,
                    "total_size_mb": total_size / (1024 * 1024),
                    "total_size_gb": total_size / (1024 * 1024 * 1024),
                }

                logger.info("storage_stats_retrieved", bucket=bucket, **stats)
                return stats
        except ClientError as e:
            logger.error(
                "storage_stats_failed",
                bucket=bucket,
                error=str(e)
            )
            raise
