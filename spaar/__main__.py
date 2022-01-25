import argparse
from threading import local
import detections
import streams
from utils import local_spark

from schemas import *

parser = argparse.ArgumentParser(description='SPAAR')
parser.add_argument("--type", help="detection or stream")
parser.add_argument("--name", help="job name")
parser.add_argument("--local", default=False, action="store_true")

DETECTIONS = {
    "cloudtrail-root_login": detections.cloudtrail.CloudTrailRootLogin
}

STREAMS = {
    "cloudtrail-parquet": streams.cloudtrail.CloudtrailParquet
}


if __name__ == "__main__":
    args = parser.parse_args()

    if args.local:
        spark = local_spark()

    if args.type == "detection":
        #TODO: detection logic
        print("detection")

        detection = DETECTIONS[args.name].detection(spark)
        detection.run()

    elif args.type == "stream":
        #TODO: stream logic
        print("stream")

        stream = STREAMS[args.name].stream
        stream = stream(spark)
        stream.run()
    else:
        #TODO error out
        print("error")