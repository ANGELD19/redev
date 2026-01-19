import base64
import boto3
import os


class S3:
    def __init__(self):
        bucket_prefix = os.getenv("S3_BUCKET_PREFIX")
        bucket_env = os.getenv("S3_BUCKET_ENV")
        self.s3_client = boto3.client("s3")
        self.bucket = f"{bucket_prefix}-private-{bucket_env}"

    def upload_file(self, file, path, file_type):
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=path,
            Body=base64.b64decode(file),
            ContentType=file_type
        )

        return f"https://{self.bucket}.s3.amazonaws.com/{path}"
    
    def get_object(self, key):
        return self.s3_client.get_object(Bucket=self.bucket, Key=key)
    
    
    def generate_presigned_url(self, key, expiration=3600):
        """
        Generates a pre-signed URL for an object in S3.

        :param key: The key for the object in the S3 bucket.
        :param expiration: The time in seconds that the URL will be valid (default 3600 seconds).
        :return: The pre-signed URL.
        """
        url = self.s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket,
                "Key": key,
            },
            ExpiresIn=expiration,
        )
        return url