from minio import Minio
import config
import os

class Storage:
    def __init__(self):
        # Minio client expects endpoint without http://
        endpoint = config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=False # Assuming internal http for now
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(config.MINIO_BUCKET):
            self.client.make_bucket(config.MINIO_BUCKET)

    def upload_file(self, local_path, object_name):
        try:
            self.client.fput_object(
                config.MINIO_BUCKET,
                object_name,
                local_path,
                content_type="image/png"
            )
            print(f"Uploaded {object_name} to {config.MINIO_BUCKET}")
            return True
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False
