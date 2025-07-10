# Azure Table Utils
A Python client for managing tables and entities in Azure Table Storage, designed to simplify interactions with Azure Storage accounts.

## Overview
The `AzureStorageTableClient` class provides a robust interface to interact with Azure Table Storage. It supports creating, deleting, and listing tables, as well as managing entities (rows) with batch operations and comprehensive error handling.

## Features
- Connect to Azure Table Storage using account name and access key.
- Create, delete, and list tables.
- Create or update entities in batches (up to 100 entities per transaction).
- Delete entities by `PartitionKey` and `RowKey`.
- Validates inputs (e.g., table names, required entity keys).
- Handles Azure service and network errors (e.g., DNS issues, invalid credentials).

## Requirements
- Python 3.7 or higher
- Azure Storage Table account with `account_name` and `access_key`
- `azure-data-tables` (included in the repository)
- `utils.py` (included in the repository)

## Installation
`pip install azure_table_client`

## Install the package
`from azure_table_client import AzureStorageTableClient, BuilderQuery, Operator`
`import os`

## Initialize with environment variables
- account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
- access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")

## Initializing the Client
client = AzureStorageTableClient(account_name=account_name, access_key=access_key)
/
/
# Method Examples
## 1. get_table_names() -> List[str]
Retrieves a list of table names from the Azure Table service.

### Returns
- **List[str]**: A list of table names in the Azure Table of the Azure Storage account.

### Raises
- **AttributeError**: If `create_connection()` was not called to initialize `table_service_client`.
- **HttpResponseError**: If the request to the Azure Table service fails (e.g., invalid credentials or table not found).
- **ServiceRequestError**: If there is a network or service connectivity issue.


## 2. create_table(self, table_name:str) -> bool:
Creates a table in the Azure Table of the Azure Storage account.

### Parameters
- **table_name** (`str`): The name for the new table that must be alphanumeric, cannot begin with a number, and must be between 3-63 characters long.

### Returns
- **bool**: True if the table was successfully created.

### Raises
- **ValueError**: If the `table_name` is not alphanumeric, is not between 3-63 characters long, or begins with a number.
- **AttributeError**: If `create_connection()` was not called to initialize `table_service_client`.
- **HttpResponseError**: If the request to the Azure Table service fails (e.g., invalid credentials or table not found).
- **ServiceRequestError**: If there is a network or service connectivity issue.


## 3. delete_table(self, table_name:str) -> bool
Checks if the table exists using the method `get_table_names()`. Deletes a table in the Azure Table of the Azure Storage account.

### Parameters
- **table_name** (`str`): The name of the table to be deleted.

### Returns
- **bool**: True if the table was successfully deleted.

### Raises
- **ValueError**: If the `table_name` is empty, invalid, or not found in the storage account.
- **AttributeError**: If `create_connection()` was not called to initialize `table_service_client`.
- **HttpResponseError**: If the request to the Azure Table service fails (e.g., invalid credentials or table not found).
- **ServiceRequestError**: If there is a network or service connectivity issue.


## 4. update_create_entity(self, table_name:str, entity:List[dict], mode:Literal['merge', 'replace']='merge') -> None
If the table does not exist, it creates. Calls the function `create_entity_batch()` to create a list of lists with a maximum of 100 entities. Creates/Updates an entity (similar to a row) to a table in the Azure Table of the Azure Storage account. If an entity with the same `PartitionKey` and `RowKey` already exists, it will be updated with the mode:
- `UpdateMode.REPLACE`: Replacing the existing entity with the given one, deleting any existing properties not included in the submitted entity.
- `UpdateMode.MERGE`: Updating the existing entity only on the properties provided as argument.

### Parameters
- **table_name** (`str`): The name of the table where the entity will be created.
- **entity** (`List[dict]`): A list of at least one dictionary, having these minimal elements `{'PartitionKey': <non-empty string>, 'RowKey': <string>}`.
- **mode** (`Literal['merge', 'replace']`): The mode for updating the entity:
  - `replace`: Replacing the existing entity with the given one, deleting any existing properties not included in the submitted entity.
  - `merge`: Updating the existing entity only on the properties provided as argument.

### Returns
- **None**

### Raises
- **ValueError**: If the `table_name` is empty or invalid, or if the `entity` is not a non-empty list of non-empty dictionaries, or if its dictionaries do not have the keys `PartitionKey` or `RowKey`.
- **AttributeError**: If `create_connection()` was not called to initialize `table_service_client`.
- **Exception**: If `create_entity_batch()` returns an error.
- **HttpResponseError**: If the request to the Azure Table service fails (e.g., invalid credentials or table not found).
- **ServiceRequestError**: If there is a network or service connectivity issue.


## 5. delete_entity(self, table_name:str, partition_key:str, row_key:str) -> bool
Deletes an entity from a specific table.

### Parameters
- **table_name** (`str`): The name of the table where the entity will be deleted from.
- **partition_key** (`str`): The name of the partition.
- **row_key** (`str`): The name of the row.

### Returns
- **bool**: True if the entity was successfully deleted.

### Raises
- **ValueError**: If the `table_name`, `partition_key`, or `row_key` is empty or invalid, or if `create_connection()` was not called to initialize `table_service_client`.
- **HttpResponseError**: If the request to the Azure Table service fails (e.g., invalid credentials or table not found).
- **ServiceRequestError**: If there is a network or service connectivity issue.


## 6. select_entity(self, table_name:str, query_filter:str, parameters: Dict[str, Any]=None, select: str|List[str]=None, results_per_page: int=None) -> Iterator
Make a query to the Azure Storage Table. You can use the class QueryBuilder to create the query.
There are two ways to create a query:
            6.1 Using only the parameter 'query_filter'
                query_filter="LastName ge 'A' and LastName lt 'B'"
            
            6.2 Using only the parameter 'query_filter' with the class QueryBuilder
                query_filter = (QueryBuilder()
                                .set_column('LastName')
                                .set_operator(Operator.GREATERTHANOREQUAL)
                                .set_value('A')
                                .set_column('LastName')
                                .set_operator(Operator.LESSTHAN)
                                .set_value('B')
                                .get_query()
                                )
            
            6.3 Using both the parameters 'query_filter' and 'parameters'
                parameters = {"first": first_name, "last": last_name}
                query_filter = "FirstName eq @first or LastName eq @last"

### Parameters
- **table_name**: `str`
  - The name of the table where the query will be made.
- **query_filter**: `str`
  - The query itself, following one of the templates provided.
- **parameters**: `dict`, optional
  - The parameters to be passed into the query if using template C.
- **select**: `str | List[str]`, optional
  - A string or list of strings representing the columns to be retrieved by the query.
- **results_per_page**: `int`, optional
  - Defines the total number of records per page in the paginated iterator. Default is 1000 records.

### Returns
- **Iterator**
  - An iterator composed of pages, with each page containing the records.

### Raises
- **ValueError**
  - If `table_name`, `partition_key`, or `row_key` is empty or invalid.
  - If `create_connection()` was not called to initialize `table_service_client`.
- **HttpResponseError**
  - If the request to the Azure Table service fails (e.g., invalid credentials or table not found).
- **ServiceRequestError**
  - If there is a network or service connectivity issue.