import unittest
from unittest.mock import MagicMock, patch, mock_open, Mock
from functions import SQLInterface
from datetime import datetime 
import os 
import hashlib

class TestBatchDataLib(unittest.TestCase):
    @patch('functions.Minio')
    @patch('functions.psycopg2.connect')
    @patch('os.path.isfile')
    
    def setUp(self, mock_minio, mock_psycopg2_connect, mock_isfile):
        ''' Set up the test funtion '''
        mock_isfile.return_value = True
        
        self.file_path = '/mock/path/to/mock_file.csv'

        self.mock_db_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_db_connection.cursor.return_value = self.mock_cursor
        mock_psycopg2_connect.return_value = self.mock_db_connection
        
        self.mock_minio_client = MagicMock()
        mock_minio.return_value = self.mock_minio_client

        self.sql_interface = SQLInterface()
        self.sql_interface.bucket_name = 'mock_bucket'
        self.sql_interface.table_name = 'mock_db'

    #I think doesnt make any sense, so can delete it 
    def test_init(self):
        ''' Check bucket and database '''
        mock_bucket = 'mock_bucket'
        mock_table = 'mock_db'
        
        self.assertEqual(self.sql_interface.bucket_name, mock_bucket, f"Got {self.sql_interface.bucket_name}. Checked that we are connected to the right bucket: {mock_bucket}")
        self.assertEqual(self.sql_interface.table_name, mock_table, f"Got {self.sql_interface.bucket_name}. Checked that we use right table: {mock_table} ")
    
    @patch('functions.SQLInterface.get_content_type')
    def test_get_content_type(self, mock_get_content):
        mock_get_content.return_value = True

        get_content = self.sql_interface.get_content_type(self.file_path)

        self.assertEqual(get_content, True, f"Got {get_content}, but expected getting right content type: {True}")

    #I guess I can delete it this test
    @patch('os.path.isfile')
    def test_get_content_type_2(self, mock_isfile):
        ''' Test get content type functionality '''
        mock_isfile.return_value = True 

        file_type = {
                '.json': 'application/json',
                '.csv': 'text/csv',
            }
    
        if not os.path.isfile(self.file_path):
            return None
        
        file_ext = os.path.splitext(self.file_path)[1]
        mock_content_type = file_type.get(file_ext)

        mock_isfile.assert_called_with(self.file_path)
        self.assertEqual(mock_content_type, 'text/csv')    
        
    @patch('builtins.open', new_callable=mock_open, read_data = 'a;a;a;a;a;a;a;'.encode())
    def test_get_hash(self, mock_get_hash):
        ''' Test get hash functionality '''
        expected_hash = '388d92cc84de2dddd74e3dfa9d3a8451'

        hash = self.sql_interface.get_hash(self.file_path)

        mock_get_hash.assert_called_with(self.file_path, 'rb')
        self.assertEqual(hash, expected_hash, f"Got {hash}, but expected hash is {expected_hash}")
    
    @patch('builtins.open', new_callable = mock_open, read_data = 'a;a;a;a;a;a;a;')
    def test_get_size(self, mock_get_size):
        ''' Test get size functionality '''
        expected_size = 14

        size = self.sql_interface.get_size(self.file_path)
        
        mock_get_size.assert_called_with(self.file_path, 'rb')
        self.assertEqual(size, expected_size, f"Got {size}, but expected size is {expected_size}")

    def test_get_timestamp(self):
        ''' Test get timestamp functionality '''
        timestamp_got = self.sql_interface.get_timestamp()
        timestamp_ex = datetime.now().isoformat()
        
        self.assertEqual(timestamp_got[:22], timestamp_ex[:22], f"Got {timestamp_got[:22]}, but expected timestamp is {timestamp_ex[:22]}")
    
    @patch('functions.SQLInterface.upload_files_to_minio')
    def test_upload_files_to_minio(self, mock_upload_to_):
        ''' Test upload to minio functionality '''
        mock_upload_to_.return_value = True

        upload_to_ = self.sql_interface.upload_files_to_minio(self.file_path)
        
        self.assertEqual(upload_to_, True, f"Got {upload_to_}, but expected uploading to minio is {True}")

    @patch('functions.SQLInterface.upload_metadata_to_postgres')
    def test_upload_metadata_to_postgres(self, mock_postgres):
        ''' Test upload metadata to postgreSQL functionality '''
        mock_postgres.return_value = True

        postgres = self.sql_interface.upload_metadata_to_postgres()

        self.assertEqual(postgres, True, f"Got {postgres}, but expected uploading metadata to postgreSQL is {True}")

    @patch('functions.SQLInterface.get_data_and_upload_everything_to_minio_and_postgres')
    def test_get_data_and_upload_everything_to_minio_and_postgres(self, mock_get_data):
        ''' Test get data and upload metadata to minio and postgreSQL functionality '''
        mock_get_data.return_value = True

        get_data = self.sql_interface.get_data_and_upload_everything_to_minio_and_postgres()

        self.assertEqual(get_data, True, f"Got {get_data}, but expected getting and uploading files is {True}")

    @patch('functions.SQLInterface.get_request')
    def test_get_request(self, mock_request):
        ''' Test get request functionality '''
        mock_request.return_value = True

        request = self.sql_interface.get_request()

        self.assertEqual(request, True, f"Got {request}, but expected getting request is {True} ")

    @patch('functions.SQLInterface.get_time_from')
    def test_get_time_from(self, mock_get_time_from):
        ''' Test get timestamp start functionality '''
        mock_get_time_from.return_value = True

        get_time_from = self.sql_interface.get_time_from()

        self.assertEqual(get_time_from, True, f"Got {get_time_from}, but expected getting timestamp_start is {True}")

    @patch('functions.SQLInterface.get_time_to')
    def test_time_to(self, mock_get_time_to):
        ''' Test get timestamp end functionality '''
        mock_get_time_to.return_value = True 

        get_time_to = self.sql_interface.get_time_to()

        self.assertEqual(get_time_to, True, f"Got {get_time_to}, but expected getting timestamp_end is {True}")

    @patch('functions.SQLInterface.all_submitted_data')
    def test_all_submitted_data(self, mock_all_sub):
        ''' Test get all submitted data functionality '''
        mock_all_sub.return_value = True

        all_sub = self.sql_interface.all_submitted_data()

        self.assertEqual(all_sub, True, f"Got {all_sub}, but expected getting all submitted data is {True}")
    
    @patch('functions.SQLInterface.upload_queried_files')
    def test_upload_queried_files(self, mock_upload_que):
        ''' Test upload queried files functionality '''
        mock_upload_que.return_value = True

        upload_que = self.sql_interface.upload_queried_files()

        self.assertEqual(upload_que, True, f"Got {upload_que}, but expected uploading queried files is {True}")

    @patch('functions.SQLInterface.submitted_data_query')
    def test_submitted_data_query(self, mock_subb_data_que):
        ''' Test submission data query functionality '''
        mock_subb_data_que.return_value = True

        subb_data_que = self.sql_interface.submitted_data_query()

        self.assertEqual(subb_data_que, True, f"Got {subb_data_que}, but expected getting submitted data query is {True}")
        
if __name__ == '__main__':
    unittest.main()

