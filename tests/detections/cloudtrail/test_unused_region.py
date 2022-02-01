import unittest
from spaar.detections.cloudtrail import UnusedRegion
from spaar.utils.spark import local_spark

SPARK = local_spark()

INPUT_DATA = "tests/detections/cloudtrail/input/unused_region.json"

class UnusedRegionTest(unittest.TestCase):
    def setUp(self):
        self._detection = UnusedRegion.detection(SPARK)
        self._detection._df = SPARK.read.schema(UnusedRegion.detection.schema).json(INPUT_DATA)

    def test_trigger(self):
        # running the trigger on test dataset
        self._detection.run_trigger()

        # asserting that there should be one row after the filter
        countDf = self._detection._df.count()
        self.assertTrue(countDf == 1)