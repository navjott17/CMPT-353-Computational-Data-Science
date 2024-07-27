Exercise 1

# Getting Started with NumPy

The NumPy data archive monthdata.npz file (in the e1.zip linked above) has two arrays containing information about precipitation in Canadian cities (each row represents a city) by month (each column is a month Jan–Dec of a particular year). The arrays are the total precipitation observed on different days, and the number of observations recorded. You can get the NumPy arrays out of the data file like this:

data = np.load('monthdata.npz')

totals = data['totals']

counts = data['counts']

Use this data to find these things:

Which city had the lowest total precipitation over the year? Hints: sum across the rows (axis 1); use argmin to determine which row has the lowest value. Print the row number.

Determine the average precipitation in these locations for each month. That will be the total precipitation for each month (axis 0), divided by the total observations for that months. Print the resulting array.

Do the same for the cities: give the average precipitation (daily precipitation averaged over the month) for each city by printing the array.

Calculate the total precipitation for each quarter in each city (i.e. the totals for each station across three-month groups). You can assume the number of columns will be divisible by 3. Hint: use the reshape function to reshape to a 4n by 3 array, sum, and reshape back to n by 4.

Write a Python program np_summary.py that produces the values specified here. Its output (with print()) should exactly match the provided np_summary.txt. We will test it on a different set of inputs: your code should not assume there is a specific number of weather stations. You can assume that there is exactly one year (12 months) of data.

# Getting Started with Pandas
To get started with Pandas, we will repeat the analysis we did with Numpy. Pandas is more data-focussed and is more friendly with its input formats. We can use nicely-formatted CSV files, and read it into a Pandas dataframe like this:

totals = pd.read_csv('totals.csv').set_index(keys=['name'])
This is the same data, but has the cities and months labelled, which is nicer to look at.

Reproduce the values you calculated with NumPy, except the quarterly totals, which are a bit of a pain. The difference will be that you can produce more informative output, since the actual months and cities are known. When you print a Pandas DataFrame or series, the format will be nicer.

Write a Python program pd_summary.py that produces the values specified here. Its output should exactly match the provided pd_summary.txt.

# Analysis with Pandas
The data in the provided files had to come from somewhere. What you got started with 180MB of data for 2016 from the Global Historical Climatology Network. To get the data down to a reasonable size, filtered out all but a few weather stations and precipitation values, joined in the names of those stations, and got the file provided as precipitation.csv.

The data in precipitation.csv is a fairly typical result of joining tables in a database, but not easy to analyse as you did above.

Create a program monthly_totals.py that recreates the totals.csv, counts.csv, and monthdata.npz files as you originally got them. The provided monthly_totals_hint.py provides an outline of what needs to happen. You need to fill in the pivot_months_pandas function (and leave the other parts intact for the next part).

Add a column 'month' that contains the results of applying the date_to_month function to the existing 'date' column. [You may have to modify date_to_month slightly, depending how your data types work out. ]
Use the Pandas groupby method to aggregate over the name and month columns. Sum each of the aggregated values to get the total. Hint: grouped_data.aggregate({'precipitation': 'sum'}).reset_index()
Use the Pandas pivot method to create a row for each station (name) and column for each month.
Repeat with the 'count' aggregation to get the count of observations.
When you submit, make sure your code is using the pivot_months_pandas function you wrote.

# Timing Comparison
Use the provided timing.ipynb notebook to test your function against the pivot_months_loops function that I wrote. (It should import into the notebook as long as you left the main function and __name__ == '__main__' part intact.)

The notebook runs the two functions and ensures that they give the same results. It also uses the %timeit magic (which uses Python timeit) to do a simple benchmark of the functions.

Run the notebook. Make sure all is well, and compare the running times of the two implementations.

#Questions
Answer these questions in a file answers.txt. [Generally, these questions should be answered in a few sentences each.]

Where you did the same calculations with NumPy and Pandas, which did you find easier to work with? Which code do you think is easier to read?
What were the running times of the two pivot_months_* functions? How can you explain the difference?
