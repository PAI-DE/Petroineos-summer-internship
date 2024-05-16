import pandas as pd
import os
from datetime import datetime

class SymbolsUpdate:
    def __init__(self):
        self.database_file = 'database.csv'

    def load_new_data_from_file(self, file_path):
        try:
            data = pd.read_csv(file_path)
            country_codes = {
                'GB': 'Great Britain',
                'IT': 'Italy',
                'BE': 'Belgium',
            }
            data['country'] = data['isin'].apply(lambda x: country_codes.get(x[:2], x[:2]))

            if 'updatedby' not in data.columns:
                data['updatedby'] = 'petroineos'

            data = data.melt(id_vars=['symbol', 'hold', 'country', 'updatedby'],
                             var_name='item', value_name='item_value')

            data['updatetime'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S.%f')

            column_order = ['symbol', 'hold', 'country', 'item', 'item_value', 'updatedby', 'updatetime']
            data = data[column_order]
            return data

        except FileNotFoundError:
            print(f"The file {file_path} does not exist.")
            return None

    def save_new_data(self, input_data):

        if os.path.exists(self.database_file):

            existing_data = pd.read_csv(self.database_file)
            combined_data = pd.concat([existing_data, input_data])
            combined_data['is_duplicated'] = combined_data.duplicated(['symbol', 'item'], keep=False)
            combined_data = combined_data.sort_values('updatetime', ascending=False)

            def handle_group(group):
                if group['hold'].nunique() == 1:
                    return group.iloc[0:1]
                else:
                    return group[group['hold'].diff() != 0].iloc[0:1]

            combined_data = combined_data.groupby(['symbol', 'item'], group_keys=False).apply(handle_group)
            combined_data.drop(columns=['is_duplicated'], inplace=True)
        else:
            combined_data = input_data

        combined_data.to_csv(self.database_file, index=False)

    def get_data_from_database(self):
        try:
            data = pd.read_csv(self.database_file)
            return data
        except FileNotFoundError:
            print("The database file does not exist.")
            return None

su = SymbolsUpdate()

files = ['symbols_update_1.csv', 'symbols_update_2.csv', 'symbols_update_3.csv']
for file_name in files:
    data = su.load_new_data_from_file(file_name)
    su.save_new_data(data)

latest_data = su.get_data_from_database()
if latest_data is not None:
    print(latest_data)
else:
    print("No data to display.")




