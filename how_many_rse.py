#!/usr/bin/env python3
#encoding: utf-8

import pandas as pd
import numpy as np

DATAFILELOC = './data/'
RSEGROUPS = 'rse_groups.csv'
UKRSE = 'association-members.csv'
UKRESEARCHERS = 'hesa_number_of_researchers_uk.csv'
JOBS = 'rse_like_jobs.csv'
RSPENDING = 'research_spending.csv'
GDP = 'gdp.csv'


def import_csv_to_df(location, filename):
    """
    Imports a csv file into a Pandas dataframe
    :params: an xls file and a sheetname from that file
    :return: a df
    """

    return pd.read_csv(location + filename, low_memory=False)


def export_to_csv(df, location, filename, index_write):
    """
    Exports a df to a csv file
    :params: a df and a location in which to save it
    :return: nothing, saves a csv
    """

    return df.to_csv(location + filename + '.csv', index=index_write)

def rse_group_average(DATAFILELOC, RSEGROUPS,num_of_groups_uk):

    # Get data on RSE Groups
    df_rse_groups = import_csv_to_df(DATAFILELOC, RSEGROUPS)
    column_names = df_rse_groups.columns

    # Median group size in data and number of groups in data
    median_group_size = round(df_rse_groups['No. of RSEs Jan 2020'].median(),0)
    num_groups_in_data = len(df_rse_groups)

    # Find missing groups
    missing_groups = num_of_groups_uk - num_groups_in_data

    # Add dummy data to make up for RSE groups not in original data
    df_extra = pd.DataFrame([[np.NaN, np.NaN, np.NaN, median_group_size, np.NaN]], columns=column_names)
    for i in range(missing_groups):
        df_rse_groups = df_rse_groups.append(df_extra, ignore_index=True)

    rses_in_groups  = df_rse_groups['No. of RSEs Jan 2020'].sum()

    return rses_in_groups


def rses_in_association(DATAFILELOC, UKRSE):

    # Get data on UKRSE Association
    df_ukrse = import_csv_to_df(DATAFILELOC, UKRSE)

    # Get last part of email address in new col
    df_ukrse['endings'] = df_ukrse['Email'].str.rsplit('.', n=1).str[1]

    # This was used in presentation, not needed for analysis
    #list_uks = df_ukrse[df_ukrse['endings']=='uk']['Email'].tolist()
    #print(set(list_uks))

    # Find all the .uk and .scot
    df_uks = df_ukrse[df_ukrse['endings'].str.contains('uk|scot')]
    uks = len(df_uks)

    # Find all the .com and .org
    df_coms_orgs = df_ukrse[df_ukrse['endings'].str.contains('com|org')]
    coms_orgs = len(df_coms_orgs)

    # Calculate how many members were in the UK by keeping all the .uk and .scot, but only
    # half of the .com and .org

    uk_rses_in_ukrse = uks + (coms_orgs/2)

    return uk_rses_in_ukrse

def researchers_in_uk(DATAFILELOC, UKRESEARCHERS):

    # Get data on UK researchers
    df_uk_research = import_csv_to_df(DATAFILELOC, UKRESEARCHERS)

    # First 28 rows of the csv are metadata! No, no, it's fine HESA. I've got tons of free time, don't you worry.
    # Tidydata, please. Tidydata!
    df_uk_research.columns = df_uk_research.iloc[27]
    df_uk_research = df_uk_research.iloc[28:]
    df_uk_research.reset_index(drop=True, inplace=True)

    # Cut to latest year
    df_uk_research = df_uk_research[df_uk_research['Academic Year']=='2018/19']

    # Cut to just the academics
    # Working with HESA data is like working with angry sharks. Given any freedom, you would choose not to, but sometimes
    # you're forced into it. They've encoded the data they need for filtering on the website into their datasets, so
    # there's massive duplication which the following five lines of code are needed to remove. Sigh.
    df_uk_research = df_uk_research[df_uk_research['Activity standard occupational classification'] == 'Total academic staff']
    df_uk_research = df_uk_research[df_uk_research['Mode of employment'] == 'All']
    df_uk_research = df_uk_research[df_uk_research['Contract marker'] == 'Academic']
    df_uk_research = df_uk_research[df_uk_research['Country of HE provider'] == 'All']
    df_uk_research = df_uk_research[df_uk_research['Region of HE provider'] == 'All']
    df_uk_research = df_uk_research[df_uk_research['HE Provider'] != 'Total']

    df_uk_research['Number'] = df_uk_research['Number'].astype(int)

    num_uk_academics = df_uk_research['Number'].sum()

    return num_uk_academics


def get_mean_rse_like_jobs(DATAFILELOC, JOBS):

    # Get the annual mean data
    df_annuals = import_csv_to_df(DATAFILELOC, JOBS)
    mean_annuals = round(df_annuals['fraction rse-like'].mean(),2)

    return mean_annuals


def we_are_not_that_big(DATAFILELOC, RSPENDING, GDP, num_rses_uk):
    """
    Both data sets are in Millions of US Dollars
    :param DATAFILELOC:
    :param RSPENDING:
    :param GDP:
    :return:
    """

    #Get data
    df_spending = import_csv_to_df(DATAFILELOC, RSPENDING)
    df_gdp = import_csv_to_df(DATAFILELOC, GDP)

    #Cut data to 2018 and drop OECD and EU28 rows
    df_spending = df_spending[df_spending['TIME']==2018]
    df_spending = df_spending[df_spending['LOCATION']!='OECD']
    df_spending = df_spending[df_spending['LOCATION'] != 'EU28']
    df_gdp = df_gdp[df_gdp['TIME']==2018]
    df_gdp = df_gdp[df_gdp['LOCATION'] != 'OECD']
    df_gdp = df_gdp[df_gdp['LOCATION'] != 'EU28']

    # Assume we're only half right about the number of RSEs in the UK
    num_rses_uk = num_rses_uk/2

    # Keep only countries for which I have spending and gdp data
    df = pd.merge(df_spending, df_gdp, on='LOCATION', how='inner', suffixes=('_spend', '_gdp'))

    # Work out how te UK compares to other countries
    df['spend by gdp'] = df['Value_spend']/df['Value_gdp']
    uk_spend_by_gdp = df.loc[df['LOCATION']=='GBR', 'spend by gdp'].tolist()[0]
    df['spend by gdp'] = df['spend by gdp']/uk_spend_by_gdp
    df['num rses'] = df['spend by gdp'] * num_rses_uk

    print(df)
    return


def main():
    """
    Main function to run program
    """

    # Number in UK RSE Groups
    num_of_groups_uk = 29
    rses_in_groups = rse_group_average(DATAFILELOC, RSEGROUPS, num_of_groups_uk)
    print('Number of RSEs in UK RSE Group: ' + str(rses_in_groups))

    # Number in UK RSE Association
    uk_rses_in_ukrse = rses_in_assoc = rses_in_association(DATAFILELOC, UKRSE)
    print('Number of UK RSEs in UKRSE Association: ' + str(uk_rses_in_ukrse))

    # Number of researchers in the UK
    num_uk_academics = researchers_in_uk(DATAFILELOC, UKRESEARCHERS)
    print('There are the following number of academics in the UK: ' + str(num_uk_academics))
    print('At a ratio of 100:1, there would be the following number of RSEs: ' + str(round(num_uk_academics/100,0)))
    print('At a ratio of 20:1, there would be the following number of RSEs: ' + str(round(num_uk_academics/20,0)))
    print('At a ratio of 3:1, there would be the following number of RSEs: ' + str(round(num_uk_academics/3,0)))

    # Number of RSE-like jobs
    rse_like_jobs = get_mean_rse_like_jobs(DATAFILELOC, JOBS)
    num_rses_uk = rse_like_jobs * num_uk_academics
    print('There are the following number of RSE-like jobs in the UK: ' + str(round(num_rses_uk,0)))

    # Scale out across the world!
    uk_as_fraction_of_world = we_are_not_that_big(DATAFILELOC, RSPENDING, GDP, num_rses_uk)

if __name__ == '__main__':
    main()
