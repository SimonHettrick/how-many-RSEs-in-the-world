#!/usr/bin/env python3
#encoding: utf-8

import pandas as pd
import numpy as np

DATAFILELOC = './data/'
RSEGROUPS = 'rse_groups.csv'
UKRSE = 'association-members.csv'
UKRESEARCHERS = 'hesa_number_of_researchers_uk.csv'


def import_csv_to_df(location, filename):
    """
    Imports a csv file into a Pandas dataframe
    :params: an xls file and a sheetname from that file
    :return: a df
    """

    return pd.read_csv(location + filename)


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

    #

if __name__ == '__main__':
    main()
