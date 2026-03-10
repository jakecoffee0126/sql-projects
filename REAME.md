https://www.youtube.com/watch?v=-NvSNqPdiPQ

#import csv file into the mysql (see the video above)

1. Open MySQL Workbench and connect to your database instance.

2. Right-click on the desired schema (database) in the Navigator panel and select "Table Data Import Wizard".

3. Select the CSV file from your local machine and click "Next".

4. Choose the destination: You can select an existing table or create a new one, which the wizard names after the file by default.

5. Configure import settings: Review and adjust the data types, column mapping, and field/line delimiters. The wizard usually makes good automatic suggestions.

6. Start the import: Click "Next" and then "Start Import". Green checkmarks indicate a successful import, and a summary will be displayed. 



## the customers.csv has a hidden charactor on the column name, like this below, so try to fix that with below code,

`ALTER TABLE customers
RENAME COLUMN `ï»¿Customer Number` TO `Customer Number`;`


## Notes:
* you have to have the ` around the column, because there is a space between two words on the column name, and SQL will treat the word "Number" as a sql