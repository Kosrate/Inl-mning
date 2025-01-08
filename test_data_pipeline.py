import unittest
import pandas as pd
from data_pipeline import process_data

class TestDataPipeline(unittest.TestCase):

    def test_process_data(self):
        # skapa exempeldata
        test_data = pd.DataFrame({
            'date': ['2024-12-01', '2024-12-02'],
        })

        # Bearbeta data
        processed_data = process_data(test_data)

        # Kontrollera att bearbetning har skett korrekt
        self.assertIn('processed_date', processed_data.columns)
        self.assertIn('status', processed_data.columns)
        self.assertEqual(processed_data['status'].iloc[0], 'processed')

    if __name__ == "__main__":
        unittest.main()