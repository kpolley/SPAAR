import unittest
from spaar.detections import cloudtrail
from spaar.utils.spark import local_spark

SPARK = local_spark()

INPUT_DATA = "tests/detections/cloudtrail/input/foreign_ip.json"

class ForeignIPTest(unittest.TestCase):
    def setUp(self):
        job = cloudtrail.ForeignIP.job
        self._detection = job(SPARK)
        self._detection._df = SPARK.read.schema(job.schema).json(INPUT_DATA)

    def test_trigger(self):
        # running the trigger on test dataset
        self._detection.run_trigger()

        # asserting that there should be one row after the filter
        countDf = self._detection._df.count()
        self.assertTrue(countDf == 1)