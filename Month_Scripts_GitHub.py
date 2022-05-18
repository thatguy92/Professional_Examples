'''
This script simply pulls the defined functions from the three listed scripts, so that I only have to run one
script every month instead of three separate ones.
'''

from Month_Stacker_Pull_GitHub import month_stacker_pull
from Month_Shared_Wallets_GitHub import month_shared_wallets
from Month_High_Volume_GitHub import month_100k


def three_scripts():
    month_stacker_pull()
    month_shared_wallets()
    month_100k()


if __name__ == '__main__':
    three_scripts()
