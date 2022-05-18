'''
This script pulls high volume customers who have made transactions in the past month and sorts them by volume in
increments of $100,000 on an Excel sheet stored on the user's desktop. While there is nothing wrong with being a high
volume customer, they do need to be monitored for other suspicious activity.
'''


import pandas as pd
import mysql.connector
import pathlib


def month_high_volume():

    try:
        desktop = pathlib.Path.home() / 'Desktop'
        month_100k_workbook = pd.ExcelWriter(str(desktop) + '/Month $100K+ Customers.xlsx')

    except FileNotFoundError:
        desktop = pathlib.Path.home() / 'OneDrive'
        month_100k_workbook = pd.ExcelWriter(str(desktop) + '/Desktop/Month $100K+ Customers.xlsx')

    amounts = [100000, 200000, 300000, 400000, 500000, 6000000, 700000, 800000, 900000, 1000000]

    mydb = mysql.connector.connect(
        host='database.company.cloud',
        database='db',
        user='user',
        password='password')

    my_cursor = mydb.cursor()

    for amount in amounts:

        my_cursor.execute(
            "SELECT uniqueid, SUM(cash) AS Total_Amount FROM customer INNER JOIN businessrecord ON "
            "customer.id = businessrecord.customer_id GROUP BY uniqueid HAVING Total_Amount >= %s AND Total_Amount < "
            "(%s+100000) AND MONTH(MAX(server_time)) > MONTH(CURRENT_DATE()) - 1 ORDER BY Total_Amount ", (amount, amount))

        hundredk_data = my_cursor.fetchall()

        hundredk_data_df = pd.DataFrame(hundredk_data)

        if not hundredk_data_df.empty:
            hundredk_data_df.columns = ["Customer ID", "Total Amount"]

            hundredk_data_df.to_excel(month_100k_workbook, "$"+str(amount)+"+", index=False)

    month_100k_workbook.save()


if __name__ == '__main__':
    month_100k()
