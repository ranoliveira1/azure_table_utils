from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import HttpResponseError, ServiceRequestError, ResourceExistsError
from typing import List, Literal, Dict, Any, Iterator
import re, copy
from .utils import ensure_attributes, ensure_non_empty_string, create_entity_batch



class Operator:
    EQUAL = 'eq'
    NOTQUAL = 'ne'
    GREATERTHAN = 'gt'
    GREATERTHANOREQUAL = 'ge'
    LESSTHAN = 'lt'
    LESSTHANOREQUAL = 'le'
    AND = 'and'
    NOT = 'not'
    OR = 'or'

    
class QueryBuilder:
    
    def __init__(self):
        self._query = ''
    
    def set_column(self, columne_name: str):
        '''
        Name of the column to operate the questy
        '''
        self._query += f"{columne_name} "
        return self
    
    def set_value(self, value: str | int | float | bool):
        '''
        General value (string, integer, float of bool) to be used in the query after the operator
        '''
        if isinstance(value, str):
            self._query += f"'{value}' "
        elif isinstance(value, (int, float)):
            self._query += f"{value} "
        elif isinstance(value, bool):
            self._query += f"{str(value).lower()} "
        return self
    
    def set_value_datetime(self, value: str):
        '''
        Value must have the format 'YYYY-MM-DDTHH:MM:SSZ'
        '''
        self._query += f"datetime{value} "
        return self

    def set_operator(self, operator: Operator):
        '''
        Operator like EQUAL to be used between a column and a value
        '''
        self._query += f"{operator} "
        return self
    
    def get_query(self):
        '''
        Retrieve a string with all components of the query
        '''
        return self._query



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
            return True
        
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to delete table "{table_name}": {str(e)}') from e


    @ensure_attributes('table_service_client')
    @ensure_non_empty_string('table_name')
    def update_create_entity(self, table_name:str, entity:List[dict], mode:Literal['merge', 'replace']='merge') -> None:
        '''
        If the table does not exist, it creates.
        Calls the function create_entity_batch() to create a list of lists with maximun of 100 entities each.
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
            A list of at least one dictionary, having this minimal elements {'PartitionKey': <non-empty string>, 'RowKey': <string>}. Additional keys must not have ponctuation or space. Any punctuation is replaced by "_"
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
            
            keys = [obj for obj in item.keys()]
            for key in keys:
                item[re.sub(r'[^a-zA-z]', '_', key)] = item.pop(key)

        try:
            self.create_table(table_name=table_name)
            
        except ResourceExistsError:
            ...
        

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
            The name of the table where the entity will be deleted from.
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
            
            return True
        except (HttpResponseError, ServiceRequestError) as e:
                raise type(e)(f'Failed to delete entity {partition_key, row_key} from the table "{table_name}": {str(e)}') from e

    
    @ensure_non_empty_string('table_name')
    @ensure_attributes('table_service_client')
    def select_entity(self, table_name:str, query_filter:str, parameters: Dict[str, Any]=None, select: str|List[str]=None, results_per_page: int=None) -> Iterator:
        '''
        Make a query to the Azure Storage Table. You can use the class QueryBuilder to create the query.
        There are two ways to create a query:
            a) Using only the parameter 'query_filter'
                query_filter="LastName ge 'A' and LastName lt 'B'"
            
            b) Using only the parameter 'query_filter' with the class QueryBuilder
                query_filter = (QueryBuilder()
                                .set_column('LastName')
                                .set_operator(Operator.GREATERTHANOREQUAL)
                                .set_value('A')
                                .set_column('LastName')
                                .set_operator(Operator.LESSTHAN)
                                .set_value('B')
                                .get_query()
                                )
            
            c) Using both the parameters 'query_filter' and 'parameters'
                parameters = {"first": first_name, "last": last_name}
                query_filter = "FirstName eq @first or LastName eq @last"
        

        Parameters
        ----------
        table_name : str
            The name of the table where the query will be make to.
        query_filter : str
            The query itself according to one of the templates ahead
        parameters : dict
            Optional. The parameters to be passed into the query if used the templace C ahead
        select : str | List[str]
            Opional. A string or a list of strings representing the columns to be retrieved by the query
        results_per_page : int
            Optional. The query returns a paginated interators. 'results_per_page' defines the total of records per page. The default is 1000 records. 
            
        Returns
        ----------
        Iterator
            Iterador made up of pages in each page having the records

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
                
        tables = self.get_table_names()
        if table_name not in tables:
            raise ValueError(f'"{table_name}" not found.')
        
        try:
            table_client = self.table_service_client.get_table_client(table_name=table_name)
        except (HttpResponseError, ServiceRequestError) as e:
            raise type(e)(f'Failed to get a Table Client for the table "{table_name}": {str(e)}') from e
        
        try:
            query = table_client.query_entities(query_filter=query_filter, parameters=parameters, select=select, results_per_page=results_per_page)
            
            return query
        except (HttpResponseError, ServiceRequestError) as e:
                raise type(e)(f'Failed to delete entity from the table "{table_name}": {str(e)}') from e

