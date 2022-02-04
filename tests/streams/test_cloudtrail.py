import unittest
from spaar.streams.cloudtrail import CloudtrailParquet 
from spaar.schemas.cloudtrail import raw, bronze
from spaar.utils.unittest import get_value_count
from spaar.utils.spark import local_spark

SPARK = local_spark()

INPUT_DATA = "tests/streams/input/cloudtrail_stream.json"

class CloudtrailTest(unittest.TestCase):
    def setUp(self):
        self._stream = CloudtrailParquet.stream(SPARK)
        self._stream._df = SPARK.read.schema(raw.schema).json(INPUT_DATA)

    def test_transform(self):
        self._stream.transform()

        # 2022-01-21T05:11:10Z
        self.assertTrue(
            get_value_count(self._stream._df, 'hr', '05') == 1
        )
        self.assertTrue(
            get_value_count(self._stream._df, 'dt', '2022-01-21') == 1
        )

        self.assertTrue(
            self._stream._df.schema == bronze.schema
        )
