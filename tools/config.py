import os

import environ


class CustomEnvironment:
    env = environ.Env()

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    environ.Env.read_env(os.path.join(BASE_DIR, ".env"))

    _aws_access_key = env.str("AWS_ACCESS_KEY")
    _aws_key_id = env.str("AWS_KEY_ID")
    _aws_s3_bucket = env.str("S3_BUCKET")
    _aws_region = env.str("AWS_REGION")

    @classmethod
    def get_aws_password(cls) -> str:
        return cls._aws_access_key

    @classmethod
    def get_aws_user(cls) -> str:
        return cls._aws_key_id

    @classmethod
    def get_aws_s3_bucket(cls) -> str:
        return cls._aws_s3_bucket

    @classmethod
    def get_aws_region(cls) -> str:
        return cls._aws_region
