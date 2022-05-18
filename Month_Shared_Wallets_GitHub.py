'''
The script finds customers that are sharing a cryptowallet. Cryptowallet sharing is an indication of people working
together to hide and launder funds. It begins by pulling all cryptowallets and customer accounts that made transactions
in the previous months. It filters out wallets that appear less than twice and pulls customer data for the remaining
accounts, grouped according to wallet addresses, and saved on an Excel workbook on the users desktop.
'''

import mysql.connector
import pandas as pd
import pathlib
from datetime import datetime


def month_shared_wallets():

    mydb = mysql.connector.connect(
        host='database.company.cloud',
        database='db',
        user='user',
        password='password')

    mycursor = mydb.cursor()

    desktop = pathlib.Path.home() / 'Desktop'
    transaction_data_workbook = pd.ExcelWriter(str(desktop) + '/Month Shared Wallets.xlsx')

    today = datetime.today()
    this_month = datetime(today.year, today.month, 1)
    last_month = datetime(today.year, today.month - 1, 1)

    emptydf = []

    mycursor.execute(
        "SELECT cryptowallet, customer.uniqueid FROM businessrecord INNER JOIN customer ON "
        "businessrecord.customer_id = customer.id WHERE cryptowallet IS NOT NULL AND businessrecord.cryptowallet "
        "NOT IN ('me56w65io67ofukms5yq3yukmdk568swei','3t4q5ytkffyot78o9rkmrk67ithatejaie') AND server_time < %s AND "
        "server_time >= %s ORDER BY cryptowallet ", (this_month, last_month))

    pull = mycursor.fetchall()

    pull = pd.DataFrame(pull)

    pull.columns = ["Wallet Address", "Public ID"]

    pull = pull.drop_duplicates()

    pull = pull.groupby("Wallet Address").filter(lambda x: len(x) > 1)

    for publicid, wallet in zip(pull["Public ID"], pull["Wallet Address"]):
        mycursor.execute(
            "SELECT cryptowallet, identity.uniqueid, customer_city, customer_state, SUM(cash) AS Money_Sent FROM "
            "businessrecord INNER JOIN customer ON businessrecord.customer_id = customer.id INNER JOIN site ON "
            "businessrecord.siteid = site.id WHERE MONTH(server_time) < MONTH(CURRENT_DATE) AND server_time >= "
            "MONTH(CURRENT_DATE) - 1 AND customer.uniqueid = %s AND cryptowallet = %s", (publicid, wallet))

        shared_wallets = mycursor.fetchall()

        shared_wallets = pd.DataFrame(shared_wallets)

        emptydf.append(shared_wallets)

    all_dfs = pd.concat(emptydf, ignore_index=True)

    all_dfs.columns = ["Wallet Address", "Public ID", "Customer City", "Customer State", "Money Sent"]

    all_dfs = all_dfs.drop_duplicates()

    all_dfs["Wallet Address"] = all_dfs["Wallet Address"].drop_duplicates()

    all_dfs.to_excel(transaction_data_workbook, "Identity Data", index=False)

    transaction_data_workbook.save()


if __name__ == '__main__':
    month_shared_wallets()
