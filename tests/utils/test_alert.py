import unittest
from spaar.utils.alert import row_to_string
from pyspark.sql import Row

class AlertTest(unittest.TestCase):
    def test_row_to_string(self):
        row = Row(eventName='test_event', eventSource='AWS', dt=2, errorMessage=None)

        expected_str = """
        {
            "eventName": "test_event",
            "eventSource": "AWS",
            "dt": 2,
            "errorMessage": null
        }
        """
        self.assertTrue(
            row_to_string(row) == expected_str
        )