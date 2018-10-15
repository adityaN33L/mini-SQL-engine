# Mini SQL Engine  

A mini sql engine which will run a subset of SQL Queries using command line interface.   

### Dataset:
1. csv files for tables. If a file is named File1.csv, the table name is File1.  
2. All the elements in files would be integers only.  
3. A file named **metadata.txt** (note the extension) is given, which has the following structure for each table:  
`<begin_table>`  
`<table_name>`  
`<attribute1>`  
...  
`<attributeN>`  
`<end_table>`  

#### Type of Queries:  
The engine runs the following set of queries: 
1. **Select all records** : select * from table_name;  
2. **Aggregate functions** : Simple aggregate functions on a single column. Sum, average, max and min. They will be very trivial given that the data is only numbers: select max(col1) from table1;  
3. **Project Columns (could be any number of columns) from one or more tables** : select col1, col2 from table_name;  
4. **Select/project with distinct from one table** : select distinct col1, col2 from table_name;  
5. **Select with where from one or more tables** : select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;  
    - In the where queries, there would be a maximum of one AND/OR operator with no NOT operators.
6. **Projection of one or more(including all the columns) from two tables with one join condition** :  
    - select * from table1, table2 where table1.col1=table2.col2;  
    - select col1,col2 from table1,table2 where table1.col1 = table2.col2;  
    - The joining column should be printed only once. No repetitions.  


#### Run Command:
bash initEngine.sh " --SQL Query-- "  
