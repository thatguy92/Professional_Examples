'''
The purpose of this script is to gather customer, location, and transaction data for suspicious activity reports (SARs).
Copying and pasting data from the web interface is a time-consuming process that leaves much room open for human error.
This easy to use script asks the user to input how many accounts they need and each account number individually. The
script then loops through 4 SQL queries, pulling data for customer information, account creation location information,
transaction information, and transaction location information. Those 4 types of data are stored in respective dataframes
which are written to sheets in an Excel workbook placed on the user's desktop. The result is that upwards of 3 hours of
manual research is done in 3 minutes at most. All sensitive information has been replaced.
'''

import mysql.connector
import pandas as pd
import numpy as np
import datetime
import pathlib
from itertools import cycle


def sar_data_pull():

    number_of_accounts = int(input("For how many accounts do you need data?:"))

    empty_df1 = []
    empty_df2 = []
    empty_df3 = []
    empty_df4 = []

    try:
        desktop = pathlib.Path.home() / 'Desktop'
        transaction_data_workbook = pd.ExcelWriter(str(desktop) + '/SAR_Data.xlsx')

    except FileNotFoundError:
        desktop = pathlib.Path.home() / 'OneDrive'
        transaction_data_workbook = pd.ExcelWriter(str(desktop) + '/Desktop/SAR_Data.xlsx')

    my_db = mysql.connector.connect(
        host='database.company.cloud',
        database='db',
        user='user',
        password='password')

    my_cursor = my_db.cursor()

    for ID in range(number_of_accounts):
        account_id = input("For which account ID do you need data?:")

        my_cursor.execute(
            "SELECT customer_firstname, customer_middlename, customer_lastname, uniqueid, phone#, idtype, id#, "
            "DATE_FORMAT(birthdate, '%m/%d/%Y'), ss#, customer_address, customer_city, customer_state, customer_zip, "
            "machine_id_created_at FROM customer LEFT JOIN customerinfo ON customer.id = "
            "customerinfo.customer_id LEFT JOIN customercellphone ON customer.id = "
            "customercellphone.customer_id WHERE uniqueid = %s LIMIT 1", (account_id,))

        identity_data = my_cursor.fetchall()

        identity_data_df = pd.DataFrame(identity_data)

        empty_df1.append(identity_data_df)

    all_dfs1 = pd.concat(empty_df1, ignore_index=True)
    all_dfs1.columns = ["First Name", "Middle Name", "Last Name", "Customer ID", "Phone Number", "ID Card Type",
                        "ID Card Number", "Date of Birth", "SSN", "Street Address", "City", "State", "Zip Code",
                        "Machine Created By"]

    all_dfs1["Phone Number"] = all_dfs1["Phone Number"].apply(lambda x: ''.join(filter(str.isdigit, x)))
    all_dfs1["Phone Number"] = all_dfs1["Phone Number"].apply(lambda x: x[-10:])

    all_dfs1["ID Card Type"] = all_dfs1["ID Card Type"].replace([1, 2, 3], ["Driver's license/State ID", "Passport",
                                                                            "Driver's license/State ID"])

    zip_list = zip(all_dfs1["Customer ID"], all_dfs1['Machine Created By']) if len(all_dfs1["Customer ID"]) > len(all_dfs1['Machine Created By']) \
        else zip(cycle(all_dfs1["Customer ID"]), all_dfs1['Machine Created By'])

    for Customer_ID, Machine in zip_list:
        my_cursor.execute(
            "SELECT machine_number, machine_address, machine_city, machine_state, machine_zip FROM machine INNER JOIN "
            "businessrecord ON machine.id = businessrecord.machine_id INNER JOIN site ON businessrecord.siteid = site.id "
            "LEFT JOIN customer ON businessrecord.customer_id = customer.id WHERE customer.uniqueid = %s "
            "AND machine.id = %s AND businessrecord != '123 Main St.' AND contactaddress != 'N/A' AND contactaddress != '' "
            "UNION SELECT machine_number, machine_address, machine_city, machine_state, machine_zip "
            "FROM machine INNER JOIN businessrecord ON  machine.id = businessrecord.machine_id INNER JOIN site ON "
            "businessrecord.siteid = site.id WHERE machine.id = %s AND contactaddress != '123 Main St.' AND "
            "contactaddress != 'N/A' AND contactaddress != '' UNION SELECT machine_number, machine_address, machine_city, "
            "machine_state, machine_zip FROM businessrecord LEFT JOIN site ON site.id = businessrecord.siteid LEFT JOIN "
            "machine ON businessrecord.machine_id = machine.id LEFT JOIN customer ON businessrecord.customer_id = customer.id "
            "WHERE customer.uniqueid = %s AND machine.id = %s LIMIT 1", (Customer_ID, Machine, Machine, Customer_ID, Machine))

        registration_location_data = my_cursor.fetchall()

        registration_location_data_df = pd.DataFrame(registration_location_data)

        empty_df2.append(registration_location_data_df)

    all_dfs2 = pd.concat(empty_df2, ignore_index=True)
    all_dfs2.columns = ['Terminal', 'Street Address', 'City', 'State', 'Zip']

    all_dfs1 = all_dfs1.drop(columns='Machine Created By')
    all_dfs1.to_excel(transaction_data_workbook, "Identity Data", index=False)
    all_dfs2 = all_dfs2.drop_duplicates()
    all_dfs2.to_excel(transaction_data_workbook, "Account Creation Location Data", index=False)

    for Customer_ID in all_dfs1['Customer ID']:
        my_cursor.execute(
            "SELECT machine_number, server_time, businessrecord.form, cash, currency_type, cryptowallet, uniqueid, "
            "crypto_type, lastupdatetime FROM businessrecord INNER JOIN customer ON businessrecord.customer_id = customer.id "
            "INNER JOIN machine ON businessrecord.machine_id = machine.id WHERE customer.uniqueid = %s ORDER BY server_time "
            "DESC", (Customer_ID, ))

        customer_transaction_data = my_cursor.fetchall()

        customer_transaction_data_df = pd.DataFrame(customer_transaction_data)

        empty_df3.append(customer_transaction_data_df)

    all_dfs3 = pd.concat(empty_df3, ignore_index=True)

    if not all_dfs3.empty:
        all_dfs3.columns = ['Terminal', 'Date', 'Transaction Type', 'Cash Amount', 'Cash Currency', 'Destination Address',
                            'Customer ID', 'Cryptocurrency Type', 'Rejected Date']
        thirty_days_prior = all_dfs3['Rejected Date'].iloc[0] - datetime.timedelta(days=31)
        all_dfs3['Date'] = all_dfs3['Date'].apply(lambda x: x.strftime('%Y/%m/%d'))
        all_dfs3['Cash Amount'] = all_dfs3['Cash Amount'].apply(lambda x: "%.2f" % x)
        all_dfs3['Cash'] = all_dfs3['Cash Amount'] + ' ' + all_dfs3['Cash Currency']
        all_dfs3['Transaction Type'] = all_dfs3['Transaction Type'].replace([0, 1, 2], ["Buy", "Sell", "Withdraw"])
        all_dfs3['Cash Amount'] = pd.to_numeric(all_dfs3['Cash Amount'])
        all_dfs3['Total Bought'] = np.sum(all_dfs3['Cash Amount'][all_dfs3['Transaction Type'] == "Buy"])
        wallet_pivot = all_dfs3.pivot_table(index=['Destination Address'], values=['Cash Amount'], aggfunc=[np.sum],
                                            margins=True)
        all_dfs3['Total Withdrawn'] = np.sum(all_dfs3['Cash Amount'][all_dfs3['Transaction Type'] == "Withdraw"])
        all_dfs3['Grand Total'] = np.sum(all_dfs3['Cash Amount'][all_dfs3['Transaction Type'] != "Sell"])
        all_dfs3['30 Day Cash Amount'] = all_dfs3['Cash Amount'][all_dfs3['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y/%m/%d'))
                                                                 >= thirty_days_prior][all_dfs3['Transaction Type'] != "Sell"]
        all_dfs3['30 Day Cash Amount'] = np.sum(all_dfs3['30 Day Cash Amount'])
        all_dfs3['# Wallet Addresses'] = len(pd.unique(all_dfs3['Destination Address']))
        all_dfs3 = all_dfs3.reindex(columns=['Terminal', 'Date', 'Transaction Type', 'Cash', 'Destination Address',
                                             'Customer ID', 'Cryptocurrency Type', 'Total Bought', 'Total Withdrawn',
                                             'Grand Total', '30 Day Cash Amount', '# Wallet Addresses'])
        all_dfs3['Date'] = all_dfs3['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y/%m/%d'))
        all_dfs3 = all_dfs3.sort_values('Date', ascending=False)
        all_dfs3['Date'] = all_dfs3['Date'].apply(lambda x: x.strftime('%m/%d/%Y'))
        all_dfs3['Cryptocurrency Type'] = all_dfs3['Cryptocurrency Type'].drop_duplicates()
        all_dfs3['Total Bought'] = all_dfs3['Total Bought'].drop_duplicates()
        all_dfs3['Total Withdrawn'] = all_dfs3['Total Withdrawn'].drop_duplicates()
        all_dfs3['Grand Total'] = all_dfs3['Grand Total'].drop_duplicates()
        all_dfs3['30 Day Cash Amount'] = all_dfs3['30 Day Cash Amount'].drop_duplicates()
        all_dfs3['# Wallet Addresses'] = all_dfs3['# Wallet Addresses'].drop_duplicates()
        all_dfs3.to_excel(transaction_data_workbook, "Transaction Data", index=False)

        zip_list2 = zip(all_dfs1["Customer ID"], all_dfs3['Terminal']) if len(all_dfs1["Customer ID"]) > len(
            all_dfs3['Terminal']) \
            else zip(cycle(all_dfs1["Customer ID"]), all_dfs3['Terminal'])

        for Customer_ID, Machine in zip_list2:
            my_cursor.execute(
                "SELECT machine_number, machine_address, machine_city, machine_state, machine_zip FROM machine INNER JOIN "
                "businessrecord ON machine.id = businessrecord.machine_id INNER JOIN site ON businessrecord.siteid = site.id "
                "LEFT JOIN customer ON businessrecord.customer_id = customer.id WHERE customer.uniqueid = %s "
                "AND machine.machine_number = %s AND businessrecord != '123 Main St.' AND contactaddress != 'N/A' AND contactaddress != '' "
                "UNION SELECT machine_number, machine_address, machine_city, machine_state, machine_zip "
                "FROM machine INNER JOIN businessrecord ON  machine.id = businessrecord.machine_id INNER JOIN site ON "
                "businessrecord.siteid = site.id WHERE machine.machine_number = %s AND contactaddress != '123 Main St.' AND "
                "contactaddress != 'N/A' AND contactaddress != '' UNION SELECT machine_number, machine_address, machine_city, "
                "machine_state, machine_zip FROM businessrecord LEFT JOIN site ON site.id = businessrecord.siteid LEFT JOIN "
                "machine ON businessrecord.machine_id = machine.id LEFT JOIN customer ON businessrecord.customer_id = customer.id "
                "WHERE customer.uniqueid = %s AND machine.machine_number = %s LIMIT 1",
                (Customer_ID, Machine, Machine, Customer_ID, Machine))

            transaction_location_data = my_cursor.fetchall()

            transaction_location_data_df = pd.DataFrame(transaction_location_data)

            empty_df4.append(transaction_location_data_df)

        all_dfs4 = pd.concat(empty_df4, ignore_index=True)
        all_dfs4.columns = ['Terminal', 'Street Address', 'City', 'State', 'Zip']

        all_dfs4 = all_dfs4.drop_duplicates()
        all_dfs4.to_excel(transaction_data_workbook, "Transaction Location Data", index=False)
        wallet_pivot.to_excel(transaction_data_workbook, "Wallet Pivot Table", index=True)

    transaction_data_workbook.save()


if __name__ == '__main__':
    sar_data_pull()
