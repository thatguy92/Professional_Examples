'''
The goal of this script is to pull customers who created multiple accounts (stackers) from the previous month. Stacking
is a user agreement violation because it is a method of hiding money as to avoid identification. The script works by
pulling all accounts created at machines in the past month and the times that they were created at (most stacked
accounts were created within 1 hour of each other). The script then filters machines that have had two accounts
created at it within the past month and further filters the accounts to those created within 1 hour of each other. The
results are stored in an Excel workbook on the users desktop. The topography that the script uses to identify stackers
pulls up some false positives, but has proven to pull over 1300 true stackers per month. This means that analysts could
spend less time manually searching for stacker accounts on the database's web interface and more time properly
documenting agreement violating accounts.
'''

import mysql.connector
import pandas as pd
import pathlib
import numpy as np


def month_stacker_pull():

    mydb = mysql.connector.connect(
        host='database.company.cloud',
        database='db',
        user='user',
        password='password')

    mycursor = mydb.cursor()

    try:
        desktop = pathlib.Path.home() / 'Desktop'
        stacker_data_workbook = pd.ExcelWriter(str(desktop) + '/Month Stackers.xlsx')

    except FileNotFoundError:
        desktop = pathlib.Path.home() / 'OneDrive'
        stacker_data_workbook = pd.ExcelWriter(str(desktop) + '/Desktop/Month Stackers.xlsx')

    mycursor.execute(
        "SELECT DISTINCT machine_number, uniqueid, created_time FROM machine INNER JOIN businessrecord ON "
        "machine.id = businessrecord.machine_id INNER JOIN customer ON businessrecord.customer_id = customer.id "
        "WHERE enhanced IS NULL AND MONTH(created_time) >= MONTH(CURRENT_DATE())-1 AND MONTH(created_time) < MONTH(CURRENT_DATE()) ")

    pull = mycursor.fetchall()

    pull = pd.DataFrame(pull)

    pull.columns = ["Machine Number", "Public ID", "Created Time"]

    pull = pull.drop_duplicates()

    pull = pull.groupby("Machine Number").filter(lambda x: len(x) > 2)

    pull = pull.sort_values(['Machine Number', 'Created Time'], ascending=False)

    pull['Time Difference'] = pull['Created Time'].diff().apply(lambda x: x / np.timedelta64(-1, 'h')).fillna(
        720).astype('int64')

    pull['Time Difference 2'] = pull['Created Time'].diff(periods=-1).apply(lambda x: x / np.timedelta64(-1, 'h')).fillna(
        720).astype('int64')

    pull = pull[(pull['Time Difference'] == 0) | (pull['Time Difference 2'] == 0)]

    pull = pull.drop(columns=['Time Difference', 'Time Difference 2'])

    pull["Machine Number"] = pull["Machine Number"].drop_duplicates()

    pull.to_excel(stacker_data_workbook, "Stacker Data", index=False)

    stacker_data_workbook.save()


if __name__ == '__main__':
    month_stacker_pull()
