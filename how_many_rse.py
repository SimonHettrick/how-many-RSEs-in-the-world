#!/usr/bin/env python3
#encoding: utf-8

import pandas as pd
import numpy as np

DATAFILELOC = './data/'
OPFILELOC = './output/'
RSEGROUPS = 'rse_groups.csv'
UKRSE = 'association-members.csv'
UKRESEARCHERS = 'hesa_number_of_researchers_uk.csv'
JOBS = 'rse_like_jobs.csv'
RSPENDING = 'research_spending.csv'
SALARY = 'salary.csv'
GLOBALRESEARCHERS = 'global_researchers.csv'
POPULATION = 'population.csv'
COUNTRYCODES = 'oecd_country_codes.csv'


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
    """
    Takes the data collected from UK RSE Groups, calculates the median group size, uses that data to make up for
    the missing groups (I got data from 25 of the 29 of them) and then calculates the total number of people in UK RSE
    Groups.
    :param DATAFILELOC: location of data files
    :param RSEGROUPS: csv with data on size of RSE Groups
    :param num_of_groups_uk: the number of RSE Groups in the UK
    :return: the total number of RSEs in UK RSE Groups
    """

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
    """
    Takes all the post-@-part of the email addresses of people signed up to the UKRSE Association, drops all the
    obviously non-UK email addresses, drops half of the .com and .org ones too. Then counts the people who are left to
    say how many UK RSEs are in the UK RSE Association.
    :param DATAFILELOC: location of data files
    :param UKRSE: csv of last parts of email addresses of people signed up to UKRSE Association
    :return: the total number of RSEs in the UKRSE Association
    """

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
    """
    Takes data from HESA and does a load of cleaning to
    :param DATAFILELOC: location of data files
    :param UKRESEARCHERS: csv of researchers in UK from HESA website
    :return: the total number of researchers in the UK
    """

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
    """
    Very simple function to calculate the mean of a few numbers related to
    RSE like jobs
    :param DATAFILELOC: location of data files
    :param JOBS: data on the mean fraction of jobs advertised on jobs.ac.uk that are RSE like
    :return: the mean of a list of fractions
    """

    # Get the annual mean data
    df_annuals = import_csv_to_df(DATAFILELOC, JOBS)
    mean_annuals = round(df_annuals['fraction rse-like'].mean(),2)

    return mean_annuals


def we_are_not_that_big(DATAFILELOC, RSPENDING, SALARY, GLOBALRESEARCHERS, POPULATION, num_rses_uk, OPFILELOC, COUNTRYCODES):
    """
    Calculates the number of RSEs worldwide. It calculates compares research spend and average salary to the UK, then
    compares number of researchers employed in the country to the UK, calculates the fractional difference between the
    UK and each country, then multiplies this by the (pretty well) understood number of RSEs in the UK.
    :param DATAFILELOC: location of data files
    :param RSPENDING: csv of research spending per country
    :param SALARY: csv of average salary per country
    :param GLOBALRESEARCHERS: csv of number of researchers per country (as percentage of total population)
    :param POPULATION: csv of population per country
    :param num_rses_uk: known number of RSEs in the UK
    :param OPFILELOC: location of output files
    :param COUNTRYCODES: csv of short country codes and full country name
    :return: a dict containing two values, each the number of RSEs in the world as calculated by one of the two methods
    """

    #Get data
    df_spending = import_csv_to_df(DATAFILELOC, RSPENDING)
    df_salary = import_csv_to_df(DATAFILELOC, SALARY)
    df_researchers = import_csv_to_df(DATAFILELOC, GLOBALRESEARCHERS)
    df_pop = import_csv_to_df(DATAFILELOC, POPULATION)
    df_countries = import_csv_to_df(DATAFILELOC, COUNTRYCODES)
    df_countries.columns = ['country', 'LOCATION']

    #Cut data to 2017 (the most recent year with the most data) and drop OECD and EU28 rows
    # Set the year of interest
    year_int = 2017
    df_spending = df_spending[df_spending['TIME']==year_int]
    df_spending = df_spending[df_spending['MEASURE']=='MLN_USD']
    df_spending = df_spending[df_spending['LOCATION']!='OECD']
    df_spending = df_spending[df_spending['LOCATION'] != 'EU28']
    df_salary = df_salary[df_salary['TIME']==year_int]
    df_researchers = df_researchers[df_researchers['TIME'] == year_int]
    df_researchers = df_researchers[df_researchers['SUBJECT'] == 'TOT']
    df_researchers = df_researchers[df_researchers['MEASURE'] == '1000EMPLOYED']
    df_researchers = df_researchers[df_researchers['LOCATION']!='OECD']
    df_researchers = df_researchers[df_researchers['LOCATION'] != 'EU28']
    df_pop = df_pop[df_pop['TIME']==year_int]
    df_pop = df_pop[df_pop['SUBJECT']=='TOT']
    df_pop = df_pop[df_pop['MEASURE'] == 'MLN_PER']

    # No salary data for China in OECD data, so have to add it (pinch of salt needed here)
    # Average salary in China in 2017 (https://www.statista.com/statistics/278349/average-annual-salary-of-an-employee-in-china/#:~:text=In%202018%2C%20an%20employee%20in,yuan%20on%20average%20in%202017.)
    av_salary = 74318
    # USD to CNY exchange rate on 31 December 2017 (https://www.xe.com/currencytables/?from=USD&date=2017-12-31)
    exg_rate = 0.1537053666
    av_salary = av_salary * exg_rate
    # Create dataframe
    salary_columns = df_salary.columns
    df_china = pd.DataFrame(columns=salary_columns)
    df_china.loc[0] = ['CHN','AVWAGE','TOT','USD',np.NaN,'2017',av_salary,np.NaN]
    # Add China data
    df_salary = df_salary.append(df_china, ignore_index=True)

    # Assume we're only half right about the number of RSEs in the UK
    num_rses_uk = num_rses_uk/2

    # Keep only countries for which I have spending and salary data
    df_spends = pd.merge(df_spending, df_salary, on='LOCATION', how='inner', suffixes=('_spend', '_salary'))

    # Calculate scaling fraction
    df_spends['spend/salary'] = df_spends['Value_spend'] / df_spends['Value_salary']
    uk_spend = df_spends.loc[df_spends['LOCATION'] == 'GBR', 'spend/salary'].tolist()[0]
    df_spends['fraction_spends'] = df_spends['spend/salary'] / uk_spend
    df_spends['num rses_spends'] = df_spends['fraction_spends'] * num_rses_uk

    # Keep only countries where I have percentage of researchers and population
    df_people = pd.merge(df_researchers, df_pop, on='LOCATION', how='inner', suffixes=('_rschrs', '_pop'))

    # Calculate scaling fraction
    # Researcher data is per 1000 employed, population data is in millions of people, so...
    df_people['Value_rschrs'] = df_people['Value_rschrs']*1000
    df_people['MEASURE_rschrs'] = '1000000EMPLOYED'
    df_people['tot_researchers'] = df_people['Value_rschrs'] * df_people['Value_pop']
    uk_researchers = df_people.loc[df_people['LOCATION'] == 'GBR', 'tot_researchers'].tolist()[0]
    df_people['fraction_researchers'] = df_people['tot_researchers'] / uk_researchers
    df_people['num rses_researchers'] = df_people['fraction_researchers'] * num_rses_uk

    # Stick into a df
    df = pd.merge(df_spends, df_people, on='LOCATION', how='outer')
    df.sort_values(by='num rses_researchers', ascending=False, inplace=True)

    # Clean up numbers
    df['num rses_spends'] = round(df['num rses_spends'], 0)
    df['num rses_researchers'] = round(df['num rses_researchers'], 0)

    # Calculate the total number of RSEs in the world
    total_worldwide_rses = {}
    total_worldwide_rses['by spends'] = round(df['num rses_spends'].sum(),0)
    total_worldwide_rses['by researchers'] = round(df['num rses_researchers'].sum(), 0)

    # Get actual country names
    df = pd.merge(df, df_countries, on='LOCATION', how='inner')

    # Export results
    export_to_csv(df, OPFILELOC, 'number_rses_by_country', False)
    # Then prettyify...
    df = df[['country', 'num rses_spends', 'num rses_researchers']]
    df['num rses_spends'].replace(np.nan, 'NO DATA', regex=True, inplace=True)
    df['num rses_researchers'].replace(np.nan, 'NO DATA', regex=True, inplace=True)
    export_to_csv(df, OPFILELOC, 'number_rses_by_country_prettified', False)

    return total_worldwide_rses


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
    total_worldwide_rses = we_are_not_that_big(DATAFILELOC, RSPENDING, SALARY, GLOBALRESEARCHERS, POPULATION, num_rses_uk, OPFILELOC, COUNTRYCODES)
    print('There are the following number of RSEs in the world: ' + str(total_worldwide_rses['by spends']) +
          ' calculated by research spend')
    print('There are the following number of RSEs in the world: ' + str(total_worldwide_rses['by researchers']) +
          ' calculated by number of researchers')

if __name__ == '__main__':
    main()
