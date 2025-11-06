import json
import os
from datetime import datetime
import sqlite_utils

class DB:

    """
    database management class. 

    """


    def __init__(self, db_name, db_path=".", delete_db=False):
        """
        Parameters:
            self.db_name
            self.delete_db
            self.db_path
        """

        self.db_name = db_name # name of record
        self.db_path = os.path.abspath(os.path.join(db_path, self.db_name))
        self.delete_db = delete_db #
        self.db = create_or_connect_to_db(self.db_path, delete_db=False)


    def create_table(self, table_name, json_path, fields, pk, fts=None):
        """
        create table or connect to db

        Parameters:
            table_name (str): name of table to created
            json_path (str): path where json is stored
            fields (dict): key,val pair of fields with values being data types
            pk (li): key field from table
            fts (li): One of more values from fields
        """

        self.table_name = table_name
        self.json_path = json_path
        self.fields = fields 
        self.pk = pk 
        self.fts = fts 

        # create table and schema
        # must include pk for fts to work
        self.db.create_table(
        self.table_name,
        self.fields
        ,pk=self.pk)

        # optional full text indexing for fields
        if self.fts is not None:
            self.db[self.table_name].enable_fts(
            self.fts,
            fts_version='FTS5'
                )
            

    def insert_json_into_table(self, table_name):
        """
        Insert json into database table. 

        Parameters:
            table_name (str): table to have data inserted into
        """

        # will not work in class since db is initialized 
        # if True, delete with function
        if self.delete_db == True:
            self.db = remove_db_if_exists(self.db_path)
            self.db = sqlite_utils.Database(self.db_path)
        else:
            # initize if db isn't too be deleted
            self.db = sqlite_utils.Database(self.db_path) # file object


        # loop through processed_output folder to 
        # list all json files
        for proc_json in os.listdir(self.json_path):

            # create full path to indivdual json file
            json_data = os.path.join(self.json_path, proc_json)

            if  json_data == os.path.join(self.json_path,f"{table_name}.json"):

                # load individual json file
                with open(json_data, 'r',  encoding='utf-8') as f:
                    json_data = json.load(f)


                try:
                    insert_data(self.db, json_data, table_name)
                except Exception as e:
                    print(e)


    def add_datetime_col_to_table(self, table_name, pk, dt_field_name="Date Created"):
        """
        add a datetime column.

        Parameters: 
            table_name(str): name of table you wish to add a datetime field
            pk(str): Primary key field, required for updating existing records 
            dt_field_name (str): pass arg to customize datetime field (dt created/updated)
        """
        
        today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        for row in self.db.query(f"select * from {table_name}"):
            row[dt_field_name] = today 

            # add param/arg 'alter=True' to update existing record
            self.db[table_name].upsert(row, pk=pk, alter=True)


    def upsert_data_into_table(self, table_name):
        """
        Updated database according to required pk

        Uses json file to make update

        Parameters: 
            table_name (str): table to be updated. 
        """

        # loop through processed_output folder to 
        # list all json files
        for proc_json in os.listdir(self.json_path):

            # create full path to indivdual json file
            json_data = os.path.join(self.json_path, proc_json)
            
            if  json_data == os.path.join(self.json_path,f"{table_name}.json"):

                # load individual json file
                with open(json_data, 'r',  encoding='utf-8') as f:
                    json_data = json.load(f)

                # upsert data function
                upsert_data(self.db_path, json_data, table_name, self.pk)
                
        self.db.close()


    def delete_data_from_table(self):
        """
        delete row or rows from databases table
        """
        pass


    def drop_table(self, table_name):
        """
        drop table from existing db

        Parameters:
            table_name (str): table name to be deleted. 
        """
        try:
            self.db[table_name].drop(ignore=True) 
        except:
            print("table doesn't exist try another name")


# FUNCTIONS
def remove_db_if_exists(db_name):
    """
    critical function for deleting function 
    if user selects the option. 

    Nested within create_or_connect_to_db function

    Returns:
        sqlite db_name
    """
    
    if os.path.exists(db_name):

        os.remove(db_name)

        return db_name
    
def create_or_connect_to_db(db_path, delete_db=False):
    """
    create or connects to sqlite db
    
    Parameters:
        delete_db (boolean): boolean value of whether to delete db.  
        db_path (str): optional db path. default kw path is "."

    Returns:
        sqlite db
    """

    if delete_db == True:
        db = remove_db_if_exists(db_path)
        db = sqlite_utils.Database(db_path)
        return db
    else:
        # initize if db isn't too be deleted
        db = sqlite_utils.Database(db_path) # file object
        return db


def insert_data(db, json_data, table_name):
    if isinstance(json_data, list):
        db[table_name].insert_all(json_data, alter=True)
    elif isinstance(json_data, dict):
        db[table_name].insert(json_data, alter=True)
    else:
        print("Error: json_data must be a list of dictionaries or a single dictionary.")


def upsert_data(db, json_data, table_name, pk):
    if isinstance(json_data, list):
        db[table_name].upsert_all(json_data, pk=pk)
    elif isinstance(json_data, dict):
        db[table_name].upsert(json_data, pk=pk)
    else:
        print("Error: json_data must be a list of dictionaries or a single dictionary.")


if __name__ == "__main__":
    
    main_table = "test_table"

    main_fields = {
                "PID": str,
                "mods.title": str,
                "mods.sm_localcorpname": str,
                "mods.sm_digital_object_identifier": str, 
                "mods.type_of_resource": str
                }
    
    main_pk = ['PID']

    main_fts = ["mods.title"]


    # db_name, db_path=".", delete_db=False
    db = DB('test.db', 'myapp')
    # table_name, json_path, fields, pk, fts=None
    db.create_table(main_table, "data", main_fields, main_pk, main_fts)
    # db.insert_json_into_table(main_table)
    