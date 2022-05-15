'''
The purpose of this script is to match the company's internal cash collection records with those of a partner bank.
Manually done, this is a tedious task that leaves much room open for human error.
This easy to use script asks the user to input the monthly sheets that they need data from given a provided Google Sheet
from a partner bank and the specific date that they need from that sheet (ex. January '21, 01/01/2021).  The script then
matches the corresponding data from the provided Google Sheet with the internal cash collection records from the
internal database based on matching machine numbers and amounts. The matched data is then placed into an Excel workbook
based on date placed on the user's desktop. Any discrepancies are denoted manually by the user and fixed accordingly
with the partner bank. This script cut the process of matching records and noting discrepancies down from 10 hours to 30
minutes. All sensitive information has been replaced.
'''
import gspread
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
import pathlib
import pandas as pd
pd.options.mode.chained_assignment = None


def kankakee_report_script():
    try:
        desktop = pathlib.Path.home() / 'Desktop'
        kankakee_workbook = pd.ExcelWriter(str(desktop) + '/Kankakee.xlsx', engine='xlsxwriter')

    except FileNotFoundError:
        desktop = pathlib.Path.home() / 'OneDrive'
        kankakee_workbook = pd.ExcelWriter(str(desktop) + '/Desktop/Kankakee.xlsx', engine='xlsxwriter')

    my_db = mysql.connector.connect(
        host='database.company.cloud',
        database='db',
        user='user',
        password='password')

    my_cursor = my_db.cursor()

    my_cursor.execute(
        "SELECT machine.machine_number,machinecashaccumulationrecord.time,terminal.label, "
        "machinecashaccumulationrecord.total,TRIM(TRAILING ', dispenser_reject = {}, recycler_missing = {}, "
        "recycler_unknown = {}' FROM machinecashaccumulationrecord.summary) AS Cash_Collection_Record FROM "
        "machinecashaccumulationrecord INNER JOIN machine ON machinecashaccumulationrecord.machine_id = machine.id "
        "INNER JOIN machine_call ON machinecashaccumulationrecord.machine_id = machine_call.machine_id "
        "WHERE machine_call.call_id = 15 AND machinecashaccumulationrecord.total > 0 ORDER BY time DESC LIMIT 5000 ")

    unclean_data = my_cursor.fetchall()

    unclean_df = pd.DataFrame(unclean_data)
    unclean_df.columns = ['Serial Number', 'Terminal Time', 'Name', 'Amount', 'Cash Collection Record']

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/spreadsheets']

    google_sheets_key_file = 'Kankakee_Key.json'  # Not included for security reasons
    credentials = ServiceAccountCredentials.from_json_keyfile_name(google_sheets_key_file, scope)
    gc = gspread.authorize(credentials)

    sheet_id = "nstrue568ew46q34gatnjsytkr7o9rjyw4351uieuwth"
    google_workbook = gc.open_by_key(sheet_id)

    days_of_the_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    for _ in days_of_the_week:
        worksheet_name = input("What is the name of the worksheet needed? (ex. January '21):")
        sheet = google_workbook.worksheet(worksheet_name)
        values = sheet.get_all_values()
        day_df = pd.DataFrame(values)
        day_df.columns = day_df.iloc[0]
        day_df.columns = ["Terminal", "Location", "Settle Date", "Revenue Date", "Deposit Amount", "Deposit Notes",
                          "Hundred's", "Fifty's", "Twenty's", "Ten's", "Five's", "Additional Notes", "Bag", "Blank 1",
                          "Blank 2", "Provisional"]
        day_df = day_df.drop(columns=["Bag", "Revenue Date", "Additional Notes", "Bag", "Blank 1", "Blank 2", "Location"])
        date = input("What is the date needed? (format: 01/01/2021):")
        day_df = day_df[day_df['Settle Date'] == date]
        day_df['Terminal'] = day_df['Terminal'].str.extract('(BT......)')
        day_df['Deposit Amount'] = day_df['Deposit Amount'].map(lambda x: x.lstrip('$ -'))
        day_df['Deposit Amount'] = day_df['Deposit Amount'].replace(',', '', regex=True)
        day_df["Deposit Amount"] = pd.to_numeric(day_df["Deposit Amount"])
        day_df = day_df[~day_df['Provisional'].astype(str).str.startswith('P')]
        day_df = day_df[~day_df['Deposit Notes'].astype(str).str.startswith('J')]
        day_df = day_df[~day_df['Deposit Notes'].astype(str).str.startswith('j')]
        day_df = day_df[~day_df['Deposit Notes'].astype(str).str.startswith('BD')]
        day_df = day_df[~day_df['Deposit Notes'].astype(str).str.startswith('-')]
        day_df['Deposit Notes'] = day_df['Deposit Notes'].str.replace('mislabeled ', '')
        date = date.replace('/', '.')
        new_df = pd.merge(day_df, unclean_df, how='left', left_on=['Deposit Amount', 'Deposit Notes'],
                          right_on=['Amount', 'Serial Number'])
        new_df["Cash Collection Record"][new_df["Deposit Notes"] == "?"] = "Part of a Later Week's Report"
        new_df['Matching Notes'] = ''
        new_df['Matching Notes'][new_df['Deposit Notes'].astype(str).str.startswith('over')] = "Double Check"
        new_df['Matching Notes'][new_df['Deposit Notes'].astype(str).str.startswith('under')] = "Double Check"
        new_df = new_df.drop(columns=['Serial Number', 'Terminal Time', 'Name', 'Amount', 'Provisional'])
        new_df.to_excel(kankakee_workbook, sheet_name=date, index=False)

    kankakee_workbook.save()


if __name__ == '__main__':
    kankakee_report_script()
