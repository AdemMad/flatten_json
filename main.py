import pandas as pd
import json
from flat_table import normalize
import os
import pyodbc
from fast_to_sql import fast_to_sql


class ParseJson:
    def __init__(self, conn, remove_null=None, max_rows=None, max_columns=None):
        self.conn = conn
        self.remove_null = remove_null
        self.max_rows = max_rows
        self.max_columns = max_columns

    def nested_columns(self, df, type):
        '''
        Returns a list of nested objects.

        Parameters
        ----------
        df : DataFrame
        type : string
            type of object to return (dictionary, list).
 
        Returns
        -------
        return_type
            List.
        '''
        dict_columns = df.applymap(lambda x: isinstance(x, type))
        result_columns = dict_columns.all()[dict_columns.all()].index.tolist()
        return result_columns


    def parse_json(self, file_path: str, object: str = None, columns: list = None, max_rows = None, max_columns = None, remove_null = None):
        '''
        Inserts DataFrame into a SQL Server table.

        Parameters
        ----------
        file : string
            File to parse.
        object : type
            object to get.
        columns : list
            columns to flatten.
        max_rows : boolean
            returns maximum rows within a frame or not.
        max_columns : boolean
            returns maximum columns within a frame or not.
            
        Returns
        -------
        return_type
            DataFrame.
        '''

        json_data = []

        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            if isinstance(data, dict):
                for lines in data[object]:
                    json_data.append(lines)
            else:
                for lines in data:
                    json_data.append(lines)

        df = pd.DataFrame(json_data)

        col_list = self.nested_columns(df, list)
        col_dict = self.nested_columns(df, dict)

        # Remove NULL values
        if self.remove_null == True:
            for col in col_list:
                df = df[df[col].astype(str) != '[]']    
 
        if columns == None:
            df = normalize(df).drop(columns='index') 
        else:
            df = normalize(df[columns]).drop(columns='index')

        if self.max_rows == True:
            pd.set_option('display.max_rows', None)
 
        if self.max_rows == True:
            pd.set_option('display.max_columns', None)

        return df


    def export_df(self, df, table_name, file):
        '''
        Inserts dataframe into a SQL Server table. 

        Parameters
        ----------
        df : DataFrame
            DataFrame to export.

        table_name : string
            Name of the SQL Server table

        file : string
            The name of the file inserted.

        Returns
        -------
        return_type
            None.
        '''

        # Create a pyodbc connection
        conn = pyodbc.connect(self.conn)

        print(f'Inserting {file} into {table_name} table...')

        # If dataframe contains rows, insert it into staging table
        if len(df) != 0:
            fast_to_sql(df, f"Stage.{table_name}", conn, if_exists="append", custom=None)

        # Commit upload actions and close connection
        conn.commit()
        conn.close()

        print(file + f'...inserted into stage.{table_name} table!')


    def rerun_pipeline(self, etl_id):
        '''
        Re-runs data pipeline based on ETL_ID.
 
        Parameters
        ----------
        etl_id : string
            ID of the ETL pipeline.

        Returns
        -------
        return_type
            None.
        '''

        dataframes = []

        for file in self.files:
            if file.replace('.json', '').endswith(str(etl_id)):
                file_path = os.path.join(self.path, file)
                df = self.parse_json(file_path, self.object, self.columns, self.max_rows, self.max_columns, self.remove_null)
                dataframes.append(df)

        df = pd.concat(dataframes).reset_index().drop(columns='index')

        return df   
    

    def flatten_file(self, file_path, object, columns):
        '''
        Flattens one single JSON file.
 
        Returns
        -------
        return_type
            DataFrame/None.
        '''

        file = os.path.basename(file_path)
        print(f'Flattening {file}...')
 
        df = self.parse_json(file_path, object, columns, self.max_rows, self.max_columns, self.remove_null)
        print(f'{file} successfully flattened!')
 
        return df
    

    def many_files(self, file_path, object, columns):
        '''
        View a concatenated dataset of many data source files.
 
        Returns
        -------
        return_type
            DataFrame.
        '''

        dataframes = []

        files = os.listdir(file_path)
 
        for file in files:
            full_path = os.path.join(file_path, file)
            df = self.parse_json(full_path, object, columns, self.max_rows, self.max_columns, self.remove_null)
            dataframes.append(df)

        df = pd.concat(dataframes).reset_index().drop(columns='index')
        return df


### Connection String details
#driver = 'ODBC Driver 17 for SQL Server'
#server = 'Your server'
#database = 'Your database'

# conn = 'DRIVER={};SERVER={};DATABASE={};Trusted_Connection=yes;'.format(driver, server, database)
# pj = ParseJson(conn, remove_null=True, max_rows=True)
# pj.many_files(r'directory', None, ['col_1', 'col_2'])