import pandas as pd

file_path = r'C:\Users\danie\OneDrive\Pulpit\vals.xlsx'
xls = pd.ExcelFile(file_path)

df = pd.read_excel(xls, sheet_name='Лист1')

filtered_addresses = df[df.iloc[:, 1].isna()].iloc[:, 0]

filtered_addresses_list = filtered_addresses.astype(str).tolist()
print(filtered_addresses_list)