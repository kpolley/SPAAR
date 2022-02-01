PROD = {
    "s3_bucket": "s3a://kpolley-datalake"
}

DEV = {
    "s3_bucket": "./datalake"
}

class Config:
    _conf = None

    @staticmethod
    def set(env_name):
        if Config._conf is None:
            Config._conf = PROD if env_name == 'prod' else DEV

    @staticmethod
    def get(config_var):
        return Config._conf.get(config_var)