import argparse

from spaar.utils import local_spark
from spaar.config import Config

parser = argparse.ArgumentParser(description='SPAAR')
parser.add_argument("--type", help="detection or stream", required=True)
parser.add_argument("--job", help="job name", required=True)
parser.add_argument("--local", default=False, action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.local:
        spark = local_spark()
        Config.set('dev')
    else:
        Config.set('prod')

    from spaar import detections
    from spaar import streams

    job_obj = eval(str(args.job))
    job_obj.job(spark).run()
    