import pandas as pd

filled_frame = pd.read_csv('../csv_files/cleaned_gapminder.csv')
# Creating copy of original dataframe
copy_filled_frame = filled_frame.copy()
column_names = [col.strip() for col in copy_filled_frame]

# Normalizing = "making range of each column same"
# Applying formula of normalizing value on each cell of each column
for column in column_names:
    if column != 'Country':
        exact_column = copy_filled_frame[column]
        maxi = exact_column.max()
        mini = exact_column.min()
        for index, value in exact_column.iteritems():
            normalize_value = (value - mini) / (maxi - mini)
            copy_filled_frame.at[index, column] = normalize_value

# Combining multiple rows into single row for each country by taking average(mean) of each column
grouped = copy_filled_frame.groupby(by='Country')
grouped = grouped.mean()

# Creating new column of same size and filling it with zero
grouped['total_pts'] = [num * 0 for num in range(grouped.shape[0])]

# Adding specific columns and saving result into new column created before
for col in ['AgriLand', 'DemocracyScore', 'Exports', 'FemaleParticipation', 'ForestArea',
            'GDPpercapita', 'HealthSpendPerPerson', 'Hightotechnologyexports', 'IncomePerPerson',
            'Internetusers', 'LifeExpectancy', 'LiteracyRateAdult', 'LIteracyRateYouth',
            'ElectricityPerPerson', 'Taxrevenue', 'Tradebalance', 'Urbanpopulationgrowth']:
    grouped['total_pts'] += grouped[col]

# Subtracting specific columns from new column created before
for col in ['Imports', 'Incomeshareofrichest10pct', 'ChildrenPerWoman', 'Trafficdeaths', 'Infantmortality',
            'Inflation', 'Populationdensity', 'YearlyCO2emission']:
    grouped['total_pts'] -= grouped[col]

# Sorting column highest to lowest value for ranking
final = grouped['total_pts'].sort_values(ascending=False).to_frame()
final['Country'] = final.index
final.drop(columns=['total_pts'], inplace=True)
# Creating new column of Ranking
final['Rank'] = [num + 1 for num in range(grouped.shape[0])]

final.to_csv('../csv_files/ranked_gapminder.csv', index=False)
