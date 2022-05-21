'''
The goal of this script is to clean files containing Bitcoin transaction history from competitors and combine them with internal Bitcoin transaction history. 
Then the combination would be stored on Google Sheets which were linked to a Tableau dashboards to visualize how the company's Bitcoin volume was compared to the
rest of the cryptocurrency market. The files containing Bitcoin transaction history from competitor were downloaded from a third-party website that tracks 
transactions on the blockchain.
'''

import pandas as pd
from datetime import datetime
import mysql.connector
import pygsheets



def automated_mutual_wallets_wb():

    google_sheets_key_file = 'Mutual_Wallets_Key.json'

    lc_raw = pd.read_csv('C:/Users/thati/Downloads/LC_Raw.csv', skiprows=8, low_memory=False)
    bcd_raw = pd.read_csv('C:/Users/thati/Downloads/BCD_Raw.csv', skiprows=8, low_memory=False)
    rc_raw = pd.read_csv('C:/Users/thati/Downloads/RC_Raw.csv', skiprows=8, low_memory=False)
    cc_raw = pd.read_csv('C:/Users/thati/Downloads/CC_Raw.csv', skiprows=8, low_memory=False)
    gc_raw = pd.read_csv('C:/Users/thati/Downloads/GC_Raw.csv', skiprows=8, low_memory=False)
    kn_raw = pd.read_csv('C:/Users/thati/Downloads/KN_Raw.csv', skiprows=8, low_memory=False)

    def auto_clean_df(df):

        df = df.drop(columns=["RISK", "TOTAL AMOUNT", "TRANSACTION ID", "RECEIVED FROM", "ENTITY TYPE", "ENTITY NAME", "GEO"])
        df.reindex(columns=['DATE TIME', 'SENT TO', 'CF_Amount', 'ADDR. AMOUNT'])

        df['DATE TIME'].fillna(method='ffill', inplace=True)
        df['DATE TIME'] = df['DATE TIME'].apply(lambda x: x[:13])
        df['DATE TIME'] = df['DATE TIME'].apply(lambda x: x.replace(',', ''))
        df['DATE TIME'] = df['DATE TIME'].str.rstrip()
        df = df[df['SENT TO'] != '']
        df = df[df['SENT TO'] != '37KraSRNCg3zBvaRs2QQUsbF6yuie7zWaj']
        df['CF_Amount'] = ''
        df['DATE TIME'] = df['DATE TIME'].apply(lambda x: datetime.strptime(x, '%b %d %Y').strftime('%m/%d/%Y'))
        df['DATE TIME'] = pd.to_datetime(df['DATE TIME'])
        df = df.groupby(pd.Grouper(key='DATE TIME', axis=0, freq='D')).sum()
        df.reindex(columns=['DATE TIME', 'SENT TO', 'CF_Amount', 'ADDR. AMOUNT'])
        return df

    lc_polished = auto_clean_df(lc_raw)
    bcd_polished = auto_clean_df(bcd_raw)
    rc_polished = auto_clean_df(rc_raw)
    cc_polished = auto_clean_df(cc_raw)
    gc_polished = auto_clean_df(gc_raw)
    kn_polished = auto_clean_df(kn_raw)

    my_db = mysql.connector.connect(
            host='database.company.cloud',
            database='db',
            user='user',
            password='password')

    my_cursor = my_db.cursor()

    my_cursor.execute('''select DATE_Format(date(convert_tz(server_time,@@session.time_zone, '+00:00')),'%c/%e/%Y') as 'CF_DATE', 
    sum(crypto) as 'CF_CRYPTO_AMT' from businessrecord where date(server_time) between 
    '2021-01-01' and DATE(Now()) and type in (0,2) and crypto_type = 'BTC' and status = 1 group by 'CF_DATE' ''')

    cf_vol_data = my_cursor.fetchall()

    cf_vol_data_df = pd.DataFrame(cf_vol_data)

    cf_vol_data_df['ADDR. AMOUNT'] = ''

    cf_vol_data_df.columns = ['DATE TIME', 'CF_Amount', 'ADDR. AMOUNT']

    cf_vol_data_df = cf_vol_data_df.reindex(columns=['DATE TIME', 'ADDR. AMOUNT', 'CF_Amount'])

    cf_vol_data_df['DATE TIME'] = pd.to_datetime(cf_vol_data_df['DATE TIME'])

    cf_vol_data_df = cf_vol_data_df.groupby(pd.Grouper(key='DATE TIME', axis=0, freq='D')).sum()

    appended_cf_vol_w_lc = cf_vol_data_df.append(lc_polished)
    appended_cf_vol_w_bcd = cf_vol_data_df.append(bcd_polished)
    appended_cf_vol_w_rc = cf_vol_data_df.append(rc_polished)
    appended_cf_vol_w_cc = cf_vol_data_df.append(cc_polished)
    appended_cf_vol_w_gc = cf_vol_data_df.append(gc_polished)
    appended_cf_vol_w_kn = cf_vol_data_df.append(kn_polished)

    def write_to_gsheet(service_file_path, spreadsheet_id, sheet_name, data_df):
        gc = pygsheets.authorize(service_file=service_file_path)
        sh = gc.open_by_key(spreadsheet_id)
        wks_write = sh.worksheet_by_title(sheet_name)
        wks_write.clear('A1', None, '*')
        wks_write.set_dataframe(data_df, (0, 0), copy_index=True, encoding='utf-8', fit=True)
        wks_write.frozen_rows = 1

    write_to_gsheet(google_sheets_key_file, 'Drtuiw5e56y7q34gaerj5w86dm6uq2m6uq2tyujw7w78', 'appended_cf_vol_w_lc', appended_cf_vol_w_lc)
    write_to_gsheet(google_sheets_key_file, '1ZbY8ARa5cdH6zXSCMZAjCVZO6GxXyiVwh79q2garaGQ', 'appended_cf_vol_w_bcd', appended_cf_vol_w_bcd)
    write_to_gsheet(google_sheets_key_file, 'Zertq2tykr68or78tq2tk7t8ot78okde5md65u6jmdtd', 'appended_cf_vol_w_rc', appended_cf_vol_w_rc)
    write_to_gsheet(google_sheets_key_file, 'm78i46hw4w5hw45yq7i776keynw45ne57ie6wenw65jm', 'appended_cf_vol_w_cc', appended_cf_vol_w_cc)
    write_to_gsheet(google_sheets_key_file, 't6uwhwsrtymd7oe67uwrtbq45y6jer8onem56ienj56w', 'appended_cf_vol_w_gc', appended_cf_vol_w_gc)
    write_to_gsheet(google_sheets_key_file, '34ty6jr68olt78o7ne5yhw45ye57i5kr7o476mj4ktrh', 'appended_cf_vol_w_kn', appended_cf_vol_w_kn)


if __name__ == '__main__':
    automated_mutual_wallets_wb()
