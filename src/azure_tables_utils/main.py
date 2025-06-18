from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import HttpResponseError, ServiceRequestError, ResourceExistsError
from typing import List, Literal
import re, copy
from utils import ensure_attributes, ensure_non_empty_string, create_entity_batch



class AzureStorageTableClient:
    '''
    A class used to connect to a Azure Storage Table and execute operations.

    Attributes
    ----------
    account_name : str
        The name of the Storage account
    access_key : str
        The access key of the Storage account
    credential : AzureNamedKeyCredential
        The credential object used for authentication with the Azure Table service, created after calling create_connection().
    table_service_client : TableServiceClient
        The client object used to interact with the Azure Table service, created after calling create_connection().

    Methods
    -------
    '''
    
    def __init__(self, account_name:str=None, access_key:str=None) -> None:
        '''
        Parameters
        ----------
        account_name : str
            The name of the Storage account
        access_key : str
            The access key of the Storage account
        '''
        self.account_name = account_name
        self.access_key = access_key
        self.table_service_client = None

        self.create_connection()
        
        
    def _get_credential(self) -> AzureNamedKeyCredential:
        '''
        Creates and returns an AzureNamedKeyCredential object if account_name and access_key are provided.
        '''

        if self.account_name == None or self.access_key == None:
            raise Exception('Both [account_name] and [access_key] must be provided.')
        
        else:
            return AzureNamedKeyCredential(self.account_name, self.access_key)
        
        
    def create_connection(self) -> None:
        '''
        Creates a connection to the Azure Table service by initializing the credential and table_service_client attributes.


        Returns
        ------
        None

        Raises
        ------
        Exception
            If either account_name or access_key is missing.
        '''
        
        self.credential = self._get_credential()

        self.table_service_client = TableServiceClient(
            endpoint=f"https://{self.account_name}.table.core.windows.net", credential=self.credential
        )
    
    
    @ensure_attributes('table_service_client')
    def get_table_names(self) -> List[str]:
        '''
        Retrieves a list of table names from the Azure Table service.


        Returns
        ----------
        List[str]
            A list of table names in the Azure Table of the Azure Storage account.

        Raises
        ------
        AttributeError
            If create_connection() was not called to initialize table_service_client.
        HttpResponseError
            If the request to the Azure Table servgice fails (e.g., invalid credentials of table not found).
        ServiceRequestError
            If there is a network or service connectivity issue.
        '''
        
        try:
            tables = self.table_service_client.list_tables()
            return [item.name for item in tables]
        
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to retrieve the table names: {str(e)}') from e


    @ensure_attributes('table_service_client')
    @ensure_non_empty_string('table_name')
    def create_table(self, table_name:str) -> bool:
        '''
        Creates a table in the Azure Table of the Azure Storage account
        

        Parameters
        ----------
        table_name : str
            The name for the new table that must be alphanumeric, cannot begin with a number and must be between 3-63 characters long.
        
        Returns
        ----------
        bool
            True if the table was successfully created.

        Raises
        ------
        ValueError
            If the 'table_name' was not alphanumeric, was not between 3-63 characters long or if it begins with a number.
        AttributeError
            If create_connection() was not called to initialize table_service_client.
        HttpResponseError
            If the request to the Azure Table servgice fails (e.g., invalid credentials of table not found).
        ServiceRequestError
            If there is a network or service connectivity issue.
        '''
        
        if not re.match(r'^[A-Za-z][A-Za-z0-9]{2,62}$', table_name):
            raise ValueError('Table name must be alphanumeric, cannot begin with a number and must be between 3-63 characters long.')
        
        try:
            self.table_service_client.create_table(table_name=table_name)
            print(f'Table "{table_name}" successfully created')
            return True
        
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to create table "{table_name}": {str(e)}') from e


    @ensure_attributes('table_service_client')
    @ensure_non_empty_string('table_name')
    def delete_table(self, table_name:str) -> bool:
        '''
        Checks if the table exists using the method get_table_names().
        Deletes a table in the Azure Table of the Azure Storage account
        

        Parameters
        ----------
        table_name : str
            The name of the table to be deleted.
        
        Returns
        ----------
        bool
            True if the table was successfully deleted.

        Raises
        ------
        ValueError
            If the table_name is empty, invalid, or not found in the storage account.
        AttributeError
            If create_connection() was not called to initialize table_service_client.
        HttpResponseError
            If the request to the Azure Table servgice fails (e.g., invalid credentials of table not found).
        ServiceRequestError
            If there is a network or service connectivity issue.
        '''
        
        tables = self.get_table_names()
        if table_name not in tables:
            raise ValueError(f'"{table_name}" not found.')
        
        try:
            self.table_service_client.delete_table(table_name=table_name)
            print(f'Table "{table_name}" successfully deleted')
            return True
        
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to delete table "{table_name}": {str(e)}') from e


    @ensure_attributes('table_service_client')
    @ensure_non_empty_string('table_name')
    def update_create_entity(self, table_name:str, entity:List[dict], mode:Literal['merge', 'replace']='merge') -> None:
        '''
        If the table does not exist, it creates.
        Calls the function create_entity_batch() to create a list of lists with maximun of 100 entities.
        Creates/Updates an entity (similar to a row) to a table in the Azure Table of the Azure Storage account
        If it already exists an entity with the same PartitionKey and RowKey, it will be updated with the mode:
            - UpdateMode.REPLACE, replacing the existing entity with the given one, deleting any existing properties not included in the submitted entity
            or 
            - UpdateMode.MERGE, updating the existing entity only on the properties provided as argument

        

        Parameters
        ----------
        table_name : str
            The name of the table where the entity will be created.
        entity : List[dict]
            A list of at least one dictionary, having this minimal elements {'PartitionKey': <non-empty string>, 'RowKey': <string>}
        mode : Literal['merge', 'replace']
            The mode up the updating the entity:
                - replace, replacing the existing entity with the given one, deleting any existing properties not included in the submitted entity
                - merge, updating the existing entity only on the properties provided as argument
        
        Returns
        ----------
        None

        Raises
        ------
        ValueError
            If the table_name is empty, invalid.
            if the entity  is not a non-empty list of non-empty dictionaries, or if its dictionaries do not have the keys 'PartitionKey' nor 'RowKey'
        AttributeError
            If create_connection() was not called to initialize table_service_client.
        Exception
            If create_entity_batch() returns error
        HttpResponseError
            If the request to the Azure Table service fails (e.g., invalid credentials of table not found).
        ServiceRequestError
            If there is a network or service connectivity issue.
        '''
        
        if not entity or not isinstance(entity, list):
            raise ValueError('Entity must be a non-empty list of non-empties dictionaries.')
        
        for item in entity:
            if not item or not isinstance(item, dict):
                raise ValueError('Entity must be a non-empty list of non-empties dictionaries.')
            if not 'PartitionKey' in item.keys() or not 'RowKey' in item.keys():
                raise ValueError('Each dictionary must have the keys "PartitionKey" and "RowKey".')
            if not isinstance(item.get('PartitionKey'), str):
                raise ValueError('The value for the key "PartitionKey" must be a non-empty string.')
        

        try:
            self.create_table(table_name=table_name)
            print(f'Created table "{table_name}"')
        except ResourceExistsError:
            print('Table already exists')
        

        try:
            table_client = self.table_service_client.get_table_client(table_name=table_name)
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to get a Table Client for the table "{table_name}": {str(e)}') from e

        try:
            batchs = create_entity_batch(entity=entity, mode=mode)
        except:
            raise Exception('Failed to create list of batchs of entities.')
        
        for batch in batchs:
            try:
                table_client.submit_transaction(batch)
                print('Entities successfully updated')
            except (HttpResponseError, ServiceRequestError) as e:
                raise type(e)(f'Failed to create entities in the table "{table_name}"; entities {batch}: {str(e)}') from e


    @ensure_non_empty_string('table_name')
    @ensure_attributes('table_service_client')
    def delete_entity(self, table_name:str, partition_key:str, row_key:str) -> bool:
        '''
        Deletes an entity from a specifc table
        

        Parameters
        ----------
        table_name : str
            The name of the table where the entity will be created.
        partition_key : str
            The name of the partition
        row_key : str
            The name of the row
            
        Returns
        ----------
        bool
            True if the table was successfully deleted.

        Raises
        ------
        ValueError
            If the table_name, partition_key or row_key is empty, invalid.
            If create_connection() was not called to initialize table_service_client.
        HttpResponseError
            If the request to the Azure Table service fails (e.g., invalid credentials of table not found).
        ServiceRequestError
            If there is a network or service connectivity issue.
        '''
        
        if not partition_key or not all([isinstance(partition_key, str), isinstance(row_key, str)]):
            raise ValueError('Table, PartitionKey and RowKey must be strings and the two firsts must be non-empties.')
        
        tables = self.get_table_names()
        if table_name not in tables:
            raise ValueError(f'"{table_name}" not found.')
        
        try:
            table_client = self.table_service_client.get_table_client(table_name=table_name)
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to get a Table Client for the table "{table_name}": {str(e)}') from e
        
        try:
            table_client.delete_entity(partition_key=partition_key, row_key=row_key)
            print(f'Entity successfully deleted')
            return True
        except (HttpResponseError, ServiceRequestError) as e:
                raise type(e)(f'Failed to delete entity {partition_key, row_key} from the table "{table_name}": {str(e)}') from e



if __name__ == '__main__':
    from time import perf_counter
    
    table_conn = AzureStorageTableClient(
        account_name='vulcansystemstorage',
        access_key='IqVni/+zdWhZOXSA5ivrjbFFKrhjBQvoGnjrHxanuWDqQ1wBlZrfHutGtSwfUM4kO1S2wntleMQy+ASttgeJDg=='
    )

    table_conn.create_connection()
    table_conn.update_create_entity(table_name='TableTest', entity=[{'PartitionKey': 'MTVH', 'RowKey': 'Action 0', 'Name': 'R'}])
    
    #CREATE TABLE
    # table_conn.create_table(table_name='TableTest')
    
    #CREATE RECORDS
    # table_conn.update_create_entity(
    #     table_name='TableTest',
    #     entity=[
    #         {'PartitionKey': 'MTVH', 'RowKey': f'Action {item}', 'Time': perf_counter()}
    #         for item in range(2000)
    #     ]
    # )


    


