import pandas as pd

data = pd.read_csv('../csv_files/Gapminder.csv')

# Removing columns that I don't want to work with to rank countries
fine_data = data.drop(
    columns=['Region-Geo', 'Year', 'Continent', 'Region', 'Year_cat', 'BodyMassIndex_M', 'BodyMassIndex_F',
             'Cellphones', 'CO2Emissions', 'EnergyUsePerPerson', 'Hourlycompensation', 'Incomeshareofpoorest10pct',
             'Longtermunemploymentrate', 'Medianage', 'MedicalDoctors', 'Murder', 'Murderedmen', 'Murderedwomen',
             'Oilconsumptionperperson', 'Populationgrowth', 'Populationtotal', 'Poverty',
             'Ratioofgirlstoboysinprimaryandsecondaryeducation', 'Renewablewater', 'Suicideage15to29',
             'Teenfertilityrate', 'TotalGDPUS', 'TotalhealthspendingperpersonUS', 'UrbanpopulationTotal'])

# Renaming some names of columns
fine_data.rename(columns={
    'AgriculturalLand': 'AgriLand',
    'Femalesaged25to54labourforceparticipationrate': 'FemaleParticipation',
    'Forestarea': 'ForestArea',
    'Governmenthealthspendingperpersontotal': 'HealthSpendPerPerson',
    'Literacyrateadulttotal': 'LiteracyRateAdult',
    'Literacyrateyouthtotal': 'LIteracyRateYouth',
    'Residentialelectricityuseperperson': 'ElectricityPerPerson'
}, inplace=True)

# Removing spaces from names of countries
# because next I'll use these names of countries as variables
# variables names must not contain spaces and dashes
fine_countries = [country.replace(" ", "") for country in fine_data['Country']]
# Removing dash "-" from names of countries
fine_countries = [country.replace("-", "") for country in fine_countries]
# Replacing entries of column "Country" with entries of fine_countries in actual data set
fine_data = fine_data.assign(Country=fine_countries)


def fill_min(column_name, country_name):
    '''
    This function will take two arguments "column_name" and "country_name",
    then find minimum value in given column of given country and
    then replace the NaN value with minimum value of that column.

    '''
    column_n = column_name
    country_n = country_name
    # keyword global() will convert the next argument string value to variable
    column = globals()[country_n][column_name]
    column = column.fillna(column.min())
    globals()[country_n][column_n] = column


def fill_mean(column_name, country_name):
    '''
    This function will take two arguments "column_name" and "country_name",
    then find mean(Average) value in given column of given country and
    then replace the NaN value with average value of that column.

    '''
    column_n = column_name
    country_n = country_name
    # keyword global() will convert the next argument string value to variable
    column = globals()[country_n][column_name]
    column = column.fillna(column.mean())
    globals()[country_n][column_n] = column


countries = fine_data['Country']  # Vector "Country" having names of countries
# Removing redundancies of country names
countries = countries.unique()

country_wise_data = []  # Will contain list of DFs, where each DF = one's country data

for country in countries:  # Here "country" = 'country name'
    # Getting all rows where value of column "Country" == 'name of country'
    country_data = fine_data.loc[(fine_data['Country'] == country)]
    country_wise_data.append(country_data)

for country_data in country_wise_data:
    country_name = country_data.iloc[0][0]
    locals()[country_name] = country_data
    for column, value in country_data.iteritems():
        if column == 'AgriLand':
            fill_mean(column, country_name)
        if column == 'ChildrenPerWoman':
            fill_mean(column, country_name)
        if column == 'DemocracyScore':
            fill_mean(column, country_name)
        if column == 'Exports':
            fill_mean(column, country_name)
        if column == 'FemaleParticipation':
            fill_mean(column, country_name)
        if column == 'ForestArea':
            fill_mean(column, country_name)
        if column == 'GDPpercapita':
            fill_mean(column, country_name)
        if column == 'HealthSpendPerPerson':
            fill_mean(column, country_name)
        if column == 'Hightotechnologyexports':
            fill_mean(column, country_name)
        if column == 'Imports':
            fill_mean(column, country_name)
        if column == 'IncomePerPerson':
            fill_mean(column, country_name)
        if column == 'Infantmortality':
            fill_mean(column, country_name)
        if column == 'Inflation':
            fill_mean(column, country_name)
        if column == 'Internetusers':
            fill_mean(column, country_name)
        if column == 'LifeExpectancy':
            fill_mean(column, country_name)
        if column == 'LiteracyRateAdult':
            fill_mean(column, country_name)
        if column == 'LIteracyRateYouth':
            fill_mean(column, country_name)
        if column == 'Populationdensity':
            fill_mean(column, country_name)
        if column == 'ElectricityPerPerson':
            fill_mean(column, country_name)
        if column == 'Taxrevenue':
            fill_mean(column, country_name)
        if column == 'Tradebalance':
            fill_mean(column, country_name)
        if column == 'YearlyCO2emission':
            fill_mean(column, country_name)
        if column == 'Incomeshareofrichest10pct':
            fill_mean(column, country_name)
        if column == 'Trafficdeaths':
            fill_mean(column, country_name)
        if column == 'Urbanpopulationgrowth':
            fill_min(column, country_name)

    # Deleting variables from memory created above by locals() inside the loop
    del locals()[country_name]

# Converting list of country wise filled and cleaned dataframe into single dataframe
filled_frame = pd.concat(country_wise_data)
# Sorting dataframe from country wise to index based
filled_frame.sort_index(inplace=True)
# Filling all columns still missing value with zero
filled_frame.fillna(0, inplace=True)

filled_frame.to_csv('../csv_files/cleaned_gapminder.csv', index=False)
