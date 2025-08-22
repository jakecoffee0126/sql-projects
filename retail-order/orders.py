# https://www.youtube.com/watch?v=uL0-6kfiH3g


#!pip install kaggle
import pandas as pd
import psycopg2
import csv

# read data from the file and handle null values
# df = pd.read_csv('orders.csv', na_values=['Not Available', 'unknown'])
df = pd.read_csv('orders.csv')
# print(df.head(20))
df['Ship Mode'].unique()


# rename columns names ..make them lower case and replace space with underscore
df.rename(columns={'order id': 'order_id'})


# To make it effiency, do below instead
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(
    ' ', '_', regex=True)
# print(df.head(5))

# write back entire data to new csv with the update title for
# each column and those 'Not Available', 'unknown'
df.to_csv('ordersNew.csv', index=False)

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
# https://www.youtube.com/watch?v=miEFm1CyjfM

# pip install psycopg2


conn = psycopg2.connect(database="postgres",
                        user="postgres",
                        host='localhost',
                        password="2diiouli",
                        port=5432)  # 5432 is the default port

# Once you have created the cursor instance, you can send commands to the database
# using the execute() method and retrieve data from a table
# using fetchone(), fetchmany(), or fetchall().

# open a cursor to perform database operations
cur = conn.cursor()

# if you don't want to drop the table directly from DB
'''
cur.execute("""
    DROP TABLE person
""")
'''


'''
Create the customer detail table
'''

# Execute a command: create a
'''

cur.execute("""
            CREATE TABLE customer_detail(
                customer_id INT PRIMARY KEY,
                customer_name VARCHAR(50) UNIQUE NOT NULL,
                age INT,
                gender CHAR
            );
""")


cur.execute("""INSERT INTO customer_detail(customer_id, customer_name, age, gender) VALUES
            (1001, 'Jake Huang', 42, 'M'),
            (1002, 'Zoe Huang', 2, 'F'),
            (1003, 'Nina Zhao', 35, 'F'),
            (1004, 'John Goodman', 38, 'M')
            """)

'''


'''

cur.execute("""
            SELECT * FROM customer_detail WHERE customer_name = 'Jake Huang';
            """)
# print(cur.fetchone())


cur.execute("""
UPDATE customer_detail SET customer_name = 'Nian X Zhao' WHERE customer_id = 1003
""")

'''

# print(cur.fetchall())
# Above print will not print out anything,
# we need to use below SELECT query to select table
# and print it out


cur.execute("""
SELECT * FROM customer_detail
""")
# print(cur.fetchall())


# rows = cur.fetchall()
# for x in rows:
#    print(x)

'''
Create the order detail table
'''

'''
cur.execute("""
            CREATE TABLE order_detail(
                order_id INT PRIMARY KEY,
                order_name VARCHAR(50) UNIQUE NOT NULL,
                order_with_customer_name VARCHAR(50) UNIQUE NOT NULL
            );
""")

cur.execute("""INSERT INTO order_detail(order_id, order_name, order_with_customer_name) VALUES
            (10, 'Hotpot', 'Jake Huang'),
            (11, 'Candy', 'Zoe Huang'),
            (12, 'Cake', 'Nina Zhao')
            """)

'''


# put order_detail before the customer_name or
# put customer_detail before the customer_name
# otherwise, we will get below error
# psycopg2.errors.AmbiguousColumn: column reference "customer_name" is ambiguous

cur.execute("""
            SELECT customer_id, order_detail.order_with_customer_name, order_id
            FROM customer_detail
            INNER JOIN order_detail
            ON customer_detail.customer_name = order_detail.order_with_customer_name
""")

# rows2 = cur.fetchall()
# for y in rows2:
#    print(y)


# load data (CSV) into the PostgreSQL
# https://www.dataquest.io/blog/loading-data-into-postgres/
# The Postgres command to load files directy into tables is called COPY
# The method to load a file into a table is called copy_from.
# Like the execute() method, it is attached to the Cursor object.
# However, it differs quite a bit from the execute() method due to its parameters.

'''
Create the retail order table to load the csv file to DB
'''

cur.execute("""
            CREATE TABLE retail_order(
                order_id INT PRIMARY KEY,
                order_date DATE,
                ship_mode VARCHAR(50),
                segment VARCHAR(50),
                country VARCHAR(50),
                city VARCHAR(50),
                state VARCHAR(50),
                postal_code VARCHAR(10),
                region VARCHAR(10),
                category VARCHAR(50),
                sub_category VARCHAR(50),
                product_id VARCHAR(50),
                cost_price VARCHAR(10),
                list_price VARCHAR(10),
                quantity VARCHAR(10),
                discount_percent VARCHAR(10)
            );
""")


with open('ordersNew.csv', 'r') as f:
    # Notice that we don't need the csv module. (NO import csv needed)
    next(f)  # Skip the header row.
    cur.copy_from(f, 'retail_order', sep=',')


'''
with open('ordersNew.csv', 'r') as f:
    data_reader = csv.reader(f)
    next(data_reader)

    for row in data_reader:
        cur.execute(
            "INSERT INTO retail_order (id, name, age) VALUES(%s, %s, %s)", row)
'''


# Make the changes to the database persistent
conn.commit()
# close cursor and communication with the database
cur.close()
conn.close()


# load data (csv) to the MySQL
# https://github.com/ankitbansal6/data_analytics_project/blob/main/orders%20data%20analysis.py
# https://www.youtube.com/watch?v =uL0-6kfiH3g&t=1274s
