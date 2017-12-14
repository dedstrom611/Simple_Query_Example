import psycopg2
import pandas as pd

''' A basic class that uses psycopg2 to execute a database query and save the results
to a Pandas dataframe.  Could be modified with additional methods in order to perform
true ETL and load transformed data to a new database.'''

class DatabaseQuery(object):
    def __init__(self, dbhost = 'database_host',
                       dbname = 'database_name',
                       dbuser = 'username',
                       dbpass = 'password'):
        ''' each instantiation of this class creates a new database connection'''
        self.conn = psycopg2.connect(host=dbhost, dbname=dbname, user=dbuser, password=dbpass)

    def __exit__(self):
        self.conn.close

    def table_exists(self, table_str):
        '''A helper method that checks whether or not a table exists in the database.
        INPUTS:
        table_str - A string referencing the table name to check

        RETURNS:
        exists - Boolean indicating True if the table exists and False if not
        '''
        exists = False
        try:
            cur = self.conn.cursor()
            cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
            exists = cur.fetchone()[0]
            print exists
            cur.close()
        except psycopg2.Error as err:
            print (err)
        return exists

    def get_table_col_names(self, table_str):
        '''A helper method that returns the column names from a table in the database.
        INPUTS:
        table_str - A string referencing the table name for which to get column names

        RETURNS:
        col_names - A list containing the column names from the table
        '''
        col_names = []
        try:
            cur = self.conn.cursor()
            cur.execute("select * from " + table_str + " LIMIT 0")
            for desc in cur.description:
                col_names.append(desc[0])
            cur.close()
        except psycopg2.Error as err:
            print (err)

        return col_names

    def get_table_names(self):
        '''A helper method that prints the names of the tables in the database.

        INPUTS:
        None
        RETURNS:
        None
        '''
        self.conn.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        for table in con.fetchall():
            print(table)

    def create_dataframe(self, query):
        '''Execute a SQL query and save the resulting query to a Pandas dataframe.

        INPUTS:
        query - A SQL query enclosed in triple-quotes.  NOTE!  This query is not
        scrubbed for potentially harmful SQL syntax.  Not to be used with user_created
        queries.

        RETURNS:
        df - A pandas dataframe of the SQL query results
        '''
        cur = self.conn.cursor()
        cur.execute(query)
        data = [i for i in cur.fetchall()]
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame.from_records(data, columns=colnames)
        return df

if __name__ == '__main__':
    '''Query the SQL sandbox for Purple Briefcase for use in job recommender engine.'''
    dbname='sandbox1'
    user='dan.edstrom'
    password='xxxxxxxxxx'
    host='127.0.0.1'

    db = DatabaseQuery(dbhost = host, dbname = dbname, dbuser = user, dbpass = password)

    pbquery = """SELECT
    	a.jobs_id,
        a.clientOrgs_id,
        a.jobType,
        a.jobCategory,
        a.jobTitle,
        a.jobCompany,
        a.jobLocation,
        a.jobState,
        a.jobDescription,
        a.jobRequirements,
        a.expiresOn,
        a.jobMajor,
        a.empJobMajor,
        a.jobSchoolOf,
        a.viewCount,
        a.uniqueViewCount,
        a.applicationCount,
        a.favoritesCount,
        b.accounts_id,
    	a.postDate,
        b.applicationDate,
        b.appMajor,
        DATE(a.postDate) AS datePosted,
        DATEDIFF(DATE(b.applicationDate), DATE(a.postDate)) AS daysToApply,
        (CASE WHEN INSTR(a.jobMajor,  b.appMajor ) >  0 AND LENGTH(b.appmajor) > 0
    		THEN 1
    		ELSE 0
        END) AS majorMatch,
    	c.state AS state_preference1,
    	c.city AS city_preference1,
    	c.altState AS state_preference2,
    	c.altCity AS city_preference2,
    	c.industry AS industry_preference,
    	c.companyType AS companyType_preference
    	FROM jobs AS a
    	LEFT JOIN jobApplications AS b
    		ON a.jobs_id= b.jobs_id
    	LEFT JOIN studentSearch AS c
    		ON b.accounts_id = c.accounts_id
    	WHERE
    			a.applicationCount > 0
    			AND (jobType LIKE "%Full Time%"
    			OR jobType LIKE "%Fellowship%"
    			OR jobType LIKE "%Research%"
    			OR jobType LIKE "%Freelance%"
    			OR jobType LIKE "%Practicum%"
    			OR jobTYPE LIKE "%Apprenticeship%")
    		AND (NOT jobType LIKE "%Volunteer%")
    		AND a.clientOrgs_id <> 1
    		AND a.clientOrgs_id <> 4
            )
    		ORDER BY a.jobs_id;
            """

    # print a list of table names
    db.get_table_names()

    # Create a dataframe of the query
    df = db.create_dataframe(pbquery)

    # Create a pickled version of the dataframe for later analysis
    df.to_pickle('../data/purple_briefcase_dataset.pkl')
