import unittest
import pandas as pd
from data_pipeline import process_data


class TestDataPipeline(unittest.TestCase):
    def test_process_data(self):
        test_data = pd.DataFrame({'date': ['2024-12-01', 'invalid-date']})
        processed_data = process_data(test_data)

        self.assertIn('processed_date', processed_data.columns)
        self.assertIn('status', processed_data.columns)
        self.assertEqual(processed_data['status'].iloc[0], 'processed')
        with self.assertRaises(Exception):  # Test för felhantering
            process_data(pd.DataFrame({'date': ['not-a-date']}))
