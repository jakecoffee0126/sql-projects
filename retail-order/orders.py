
#!pip install kaggle
import pandas as pd


# read data from the file and handle null values
df = pd.read_csv('orders.csv', na_values=['Not Available', 'unknown'])
# print(df.head(20))
df['Ship Mode'].unique()


# rename columns names ..make them lower case and replace space with underscore
df.rename(columns={'order id': 'order_id'})


# To make it effiency, do below instead
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(
    ' ', '_', regex=True)
# print(df.head(5))


# derive new columns discount , sale price and profit
df['discount_price'] = df['list_price']*df['discount_percent']*.01

df['sale_price'] = df['list_price']-df['discount_price']

df['profit_price'] = df['sale_price'] - df['cost_price']

# convert order date from object data type to datetime
'''
because when we load it to the SQL, we want it to the date,
in that case, we will be able to do the operation in SQL
'''
df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')
# print(df['order_date'])


# drop cost price list price and discount percent columns
# drop will not drop the original table set, use inplace to delete from the original table
df.drop(columns=['list_price', 'cost_price', 'discount_price'], inplace=True)
print(df)

# load the data into sql server using replace option
# The most classical and well documented library to tap into PostgreSQL from python is probably psycopg2
# https://www.datacamp.com/tutorial/tutorial-postgresql-python
# pip install psycopg2
