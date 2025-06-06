import psycopg2 
import configparser
import os
from minio import Minio
import hashlib
from datetime import datetime, time
from flask import request
import pytz
from psycopg2 import sql


class SQLInterface():
    def __init__(self, config_path = 'config.conf'):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        minio_config = self.config['Minio']
        self.bucket_name = minio_config['bucket_name']
        self.folder_path = minio_config['folder_path']

        db_config = self.config['Database']
        self.table_name = db_config['db_name']
        self.columns = list(self.config['Columns'].keys())

        self.UTC =pytz.utc
        
        self.client = Minio( 
            minio_config['host'], 
            access_key= minio_config['access_key'], 
            secret_key= minio_config['secret_key'],
            secure= False
        )

        self.database_connection = psycopg2.connect(
            user = db_config['db_user'],
            password = db_config['db_password'],
            host = db_config['db_host'],
            port = db_config['db_port'],
            database = db_config['db_name']
        )

        self.cursor = self.database_connection.cursor()

    def get_content_type(self):
        file_type = {
                '.json': 'application/json',
                '.csv': 'text/csv',
            }
        
        #if file is not in the right path 
        if not os.path.isfile(self.file_path):
            return None
        
        #get a file extension 
        file_ext = os.path.splitext(self.file_path)[1]
        return file_type.get(file_ext)
    
    #get hash of a file 
    def get_hash(self):
        #open file, read it and transform received data to the hash
        with open(self.file_path, 'rb') as f:
                file_data = f.read()
                file_hash = hashlib.md5(file_data).hexdigest()
        return file_hash
    
    #get size of a file 
    def get_size(self):
        #open file, read it and get file_size 
        with open(self.file_path, 'rb') as f:
                file_data = f.read()
                file_size = len(file_data)
        return file_size

    #get timestamp for last_upload_timestamp and upload_timestamp. It works for both, because in sql if we already has a upload_timestamp it dont get changed 
    def get_timestamp(self):
        return datetime.now().isoformat()
    
    #upload files and metadata to minio 
    def upload_files_to_minio(self):
         #open all the files in dir and insert all nided data to minio
         with open(self.file_path, 'rb') as file_data:
                self.client.put_object(
                    bucket_name = self.bucket_name,
                    object_name = self.file_name,
                    data = file_data,
                    length = self.file_size,
                    metadata=self.metadata
            )
    
    #upload metadata to postgreSQL
    def upload_metadata_to_postgres(self):
        self.cursor.execute(sql.SQL("""INSERT INTO {table_name} ({column1}, {column2}, {column3}, {column4}, {column5}, {column6}, {column7}, {column8}, {column9}) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT ({column2}) DO UPDATE SET {column1}=EXCLUDED.{column1},
                                    {column2}=EXCLUDED.{column2},
                                    {column3}=EXCLUDED.{column3},
                                    {column4}=EXCLUDED.{column4},
                                    {column5}=EXCLUDED.{column5},
                                    {column6}=COALESCE({table_name}.{column6}, EXCLUDED.{column6}),
                                    {column7}=EXCLUDED.{column7},
                                    {column8}=EXCLUDED.{column8},
                                    {column9}=EXCLUDED.{column9}""").format(table_name=sql.Identifier(self.table_name), 
                                                                            column1 = sql.Identifier(self.columns[1]),
                                                                            column2 = sql.Identifier(self.columns[2]),
                                                                            column3 = sql.Identifier(self.columns[3]),
                                                                            column4 = sql.Identifier(self.columns[4]),
                                                                            column5 = sql.Identifier(self.columns[5]),
                                                                            column6 = sql.Identifier(self.columns[6]),
                                                                            column7 = sql.Identifier(self.columns[7]),
                                                                            column8 = sql.Identifier(self.columns[8]),
                                                                            column9 = sql.Identifier(self.columns[9])
                                                                            ),
                            (
                            self.metadata['bucket_name'],
                            self.metadata['storage_address'],
                            self.metadata['owner'],
                            self.metadata['file_name'],
                            self.metadata['file_size'],
                            self.metadata['upload_timestamp'],
                            self.metadata['hash_checksum'],
                            self.metadata['last_modified_timestamp'],
                            self.metadata['content_type'],
                           ))
        self.database_connection.commit()

    #all previous functions in one module which get data and upload to minio and postgreSQL
    def get_data_and_upload_everything_to_minio_and_postgres(self):
        #make a loop for each file which needs to be uploaded 
        for self.file_name in os.listdir(self.folder_path):
            self.file_path = os.path.join(self.folder_path, self.file_name)

            #get all needed parts of each file
            self.content_type = self.get_content_type()
            if self.content_type is None:
                continue

            self.file_hash = self.get_hash()
            self.file_size = self.get_size()
            self.upload_timestamp = self.get_timestamp()

            #each part of a file get imported to metadata
            self.metadata = {
                'bucket_name': self.bucket_name,
                'storage_address': f"{self.bucket_name}/{self.file_name}",
                'owner': 'admin',
                'file_name': self.file_name,
                'file_size': str(self.file_size),
                'upload_timestamp': self.upload_timestamp, 
                'hash_checksum': self.file_hash,
                'last_modified_timestamp': self.upload_timestamp,
                'content_type': self.content_type
            }

            #upload all the files to minio 
            self.upload_files_to_minio()

            #upload metadata to postgreSQL
            self.upload_metadata_to_postgres()

            #logs
            print(f"Metadata for file '{self.file_name}': {self.metadata}")

    #get requested parameters from HTML page 
    def get_request(self):
        self.file_name = request.form["file_name"]    
        self.content_type = request.form["content_type"]
        self.upload_timestamp_from = request.form['upload_timestamp_from']
        self.upload_timestamp_to = request.form['upload_timestamp_to']
    
    #transform upload_timestamp_from to use it with a upload_timestamp
    def get_time_from(self):
        if self.upload_timestamp_from:
                self.upload_timestamp_from = datetime.strptime(self.upload_timestamp_from, "%Y-%m-%d") 
                self.upload_timestamp_from = datetime.combine(self.upload_timestamp_from.date(), time.min)
                self.upload_timestamp_from = self.UTC.localize(self.upload_timestamp_from)

        return self.upload_timestamp_from
    
    #transform upload_timestamp_to to use it with a upload_timestamp
    def get_time_to(self):
        if self.upload_timestamp_to:
                self.upload_timestamp_to = datetime.strptime(self.upload_timestamp_to, "%Y-%m-%d") 
                self.upload_timestamp_to = datetime.combine(self.upload_timestamp_to.date(), time.max)
                self.upload_timestamp_to = self.UTC.localize(self.upload_timestamp_to)

        return self.upload_timestamp_to
    
    #get all received data in one module
    def all_submitted_data(self):
            self.get_request()
            self.get_time_from()
            self.get_time_to() 
        
    #apply all the parameters to get suitable files 
    def submitted_data_query(self):

        #load what we got from requests
        self.all_submitted_data()

        #makes query, filters and parameters to find all suitable files for our query 
        self.query = sql.SQL("SELECT {column4}, {column6}, {column9} FROM {table_name}").format(table_name = sql.Identifier(self.table_name),
                                                                                                    column4 = sql.Identifier(self.columns[4]),
                                                                                                    column6 = sql.Identifier(self.columns[6]),
                                                                                                    column9 = sql.Identifier(self.columns[9]))            
        self.filter = []
        self.params = []
        
        #function which applies requested data and our metadata to get a suitable files from postgreSQL
        conditions = {
            self.file_name: (sql.SQL("file_name ILIKE %s"), f"%{self.file_name}%"),
            self.content_type: (sql.SQL("content_type ILIKE %s"), f"%{self.content_type}%"),
            self.upload_timestamp_from: (sql.SQL("upload_timestamp >= %s::timestamptz"), self.upload_timestamp_from),
            self.upload_timestamp_to: (sql.SQL("upload_timestamp <= %s::timestamptz"), self.upload_timestamp_to),
        }

        #if have params from request for each of them use function from conditions 
        for key, (condition, param) in conditions.items():
            if key:
               self.filter.append(condition)
               self.params.append(param)

        #get files from postgreSQL with received filters and parameters 
        if self.params:
            self.query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(self.filter)
        
        #log
        print(f"SQL query: {self.query}")
        print(f"Params: {self.params}")
        
        #apply query to get suitable files from postgreSQL 
        self.cursor.execute(self.query, self.params)
        files = self.cursor.fetchall()
        
        #log
        print(f"Fetched files: {files}")

        #make list of files which is suitable and return it to show using jsonify
        self.filtered_files = [
            {
                "file_name": file[0],
                "content_type": file[2],
                "upload_timestamp": file[1].strftime("%Y-%m-%d %H-:%M:%S %Z"),
            }   
            for file in files]

        return self.filtered_files
    

        
        


        
        






             
            
            
            


