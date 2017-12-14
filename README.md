# Simple_Query_Example
A basic Python example of querying a database and saving the results to a Pandas dataframe

This sample class uses psycopg2 to establish a database connection.  There are several
helper methods that check for table existence and return or print lists of tables or columns.

### Notes

+ The execution of SQL queries is done without pre-processing or scrubbing of inputs.  As such, queries from user input should not be used.
+ The class contains a method to turn the results of a query into a Pandas dataframe.  Additional methods could be added that would allow for writing back to the database or to another database.
