'''
This script pulls suspicious activity report (SAR) data from a Google sheet and creates pivot tables to measure KPIs.
These KPIs where visualized as bar charts using a defined function, and one was defined as a pie chart. These
visualizations were presented at Monthly board meetings with C-level executives to show department performance and
determine strategic next steps. All sensitive information has been replaced.
'''

import gspread
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/spreadsheets']

GOOGLE_SHEETS_KEY_FILE = 'Kankakee_Key.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_KEY_FILE, scope)
gc = gspread.authorize(credentials)

sheet_id = "nsrtie658972zsetrq325w6kfyufo0r67yue74q5yhsy"
google_workbook = gc.open_by_key(sheet_id)

worksheet_name = int(input("What is the name of the worksheet needed? (ex. October '20):"))
sheet = google_workbook.worksheet(worksheet_name)
values = sheet.get_all_values()
Month_df = pd.DataFrame(values)
Month_df.columns = Month_df.iloc[0]
Month_df = Month_df.drop(Month_df.index[0])
Month_df['Tx amount (current month)'] = Month_df['Tx amount (current month)'].str.replace('$', '')
Month_df['Tx amount (current month)'] = Month_df['Tx amount (current month)'].str.replace(',', '')
Month_df['Tx amount (current month)'] = pd.to_numeric(Month_df['Tx amount (current month)'])
Month_df['Tx Life'] = Month_df['Tx Life'].str.replace('$', '')
Month_df['Tx Life'] = Month_df['Tx Life'].str.replace(',', '')
Month_df['Tx Life'] = pd.to_numeric(Month_df['Tx Life'])

Month_pivot = Month_df[Month_df['Rejected?'] == 'Yes'].pivot_table(index=['Primary Reason'],
                                                                   values=['Tx amount (current month)'],
                                                                   aggfunc=[np.count_nonzero, np.sum, np.mean, np.max],
                                                                   margins=True)

Month_pivot = Month_pivot.drop(Month_pivot.index[0])
print(Month_pivot)

Month_df1 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Tx amount (current month)'].sum()
print(Month_df1)
Month_df2 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Rejected?'].count()
print(Month_df2)
Month_df3 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Tx amount (current month)'].mean()
print(Month_df3)
Month_df4 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Tx amount (current month)'].max()
print(Month_df4)
Month_df5 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Rejected By?')['Tx amount (current month)'].sum()
print(Month_df5)
Month_df6 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby(['Rejected By?'])['Rejected?'].count()
print(Month_df6)
Month_df7 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Tx Life'].mean()
print(Month_df7)
Month_df8 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Tx Life'].max()
print(Month_df8)
Month_df9 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Primary Reason')['Tx Life'].sum()
print(Month_df9)
Month_df10 = Month_df[Month_df['Rejected?'] == 'Yes'].groupby('Rejected By?')['Tx Life'].sum()
print(Month_df10)

Month_workbook = pd.ExcelWriter('C:/Users/thati/Desktop/Test_Monthly.xlsx')
Month_pivot.to_excel(Month_workbook, 'SAR Pivot Table')
Month_df1.to_excel(Month_workbook, 'Rejected Total Amounts')
Month_df2.to_excel(Month_workbook, 'Rejected Total Counts')
Month_df3.to_excel(Month_workbook, 'Rejected Total Averages')
Month_df4.to_excel(Month_workbook, 'Rejected Max Amounts')
Month_df5.to_excel(Month_workbook, 'Transaction Totals by Dept.')
Month_df6.to_excel(Month_workbook, 'Counts C.S. vs Comp')
Month_df7.to_excel(Month_workbook, 'Rejected Total Amounts (Life)')
Month_df8.to_excel(Month_workbook, 'Trans. Totals by Dept. (Life)')
Month_df9.to_excel(Month_workbook, 'Rejected Total Averages (Life)')
Month_df10.to_excel(Month_workbook, 'Rejected Max Amounts (Life)')
Month_workbook.save()


def bar_graph(dataframe):
    plt.clf()
    dataframe.plot(kind='barh')
    plt.title("Trans. Totals of Rejected Types")
    plt.xlabel("Rejection Type")
    plt.ylabel("Transaction Totals")
    plt.show()


for df in [Month_df1, Month_df2, Month_df3, Month_df4, Month_df5, Month_df6, Month_df7, Month_df8, Month_df9, Month_df10]:
    bar_graph(df)

plt.clf()
Month_df1.plot(kind='pie')
plt.title("Trans. Totals of Rejected Types")
plt.show()