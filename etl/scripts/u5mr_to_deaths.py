"""
Script to quick-and-dirty calculate number of under 5 deaths from U5MR and population data.
For each year where U5MR is available but population by age is not, % population under 5 is backfilled (no forward fill!).
I.e. we assume the percentage of 5 year olds in the far past (often 1800-1950) is same as the first available datapoint (often 1950)

This script uses U5MR from this repo and expects you have the following datasets in your filesystem as siblings of this dataset:
 - ddf--gapminder--systema_globalis for long population time series
 - ddf--gapminder--population for population by age
"""

import pandas as pd
import glob

def getu1pop():
    path = r'./../../../ddf--gapminder--population/ddf--datapoints--population--by--country--age--year'
    all_files = glob.glob(path + "/*.csv")

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df = df[df.age < 1].drop(columns='age')
    df = df.groupby(['country','year']).sum()
    df = df.reset_index(drop=False)
    df = df.rename(columns={'year': 'time', 'population': 'u1pop' })
    return df

def main():
    u5mr = pd.read_csv('../../ddf--datapoints--child_mortality_0_5_year_olds_dying_per_1000_born--by--geo--time.csv')
    u5mr = u5mr.rename(columns={ 'geo': 'country'})
    u1pop = getu1pop()
    pop = pd.read_csv('../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv')
    pop = pop.rename(columns={ 'geo': 'country', 'year': 'time'})

    df = u5mr.merge(u1pop, on=['country','time'], how='outer')
    df = df.merge(pop, on=['country','time'], how='outer')
    df['u1poppercent'] = df['u1pop'] / df['population_total']
    df['u1poppercent'] = df['u1poppercent'].fillna(method='bfill')
    df['u1pop_new'] = df['u1poppercent'] * df['population_total']
    df['u5deaths'] = df['child_mortality_0_5_year_olds_dying_per_1000_born'] / 1000 * df['u1pop_new']
    df['u5deaths'] = df['u5deaths'].round()

    result = df[['country', 'time', 'u5deaths']]
    result.to_csv('u5deaths.csv', index=False, float_format='%d')
    df.to_csv('full.csv', index=False)
    return df

main()

