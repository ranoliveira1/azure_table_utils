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

pip install azure_table_client



## Initializing the Client
Create an instance of AzureStorageTableClient with your account name and access key.
python


## Install the package
from azure_table_client import AzureStorageTableClient, BuilderQuery, Operator

import os


## Initialize with environment variables
- account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
- access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")

client = AzureStorageTableClient(account_name=account_name, access_key=access_key)

Method Examples
1. create_connection()
Establishes a connection to the Azure Table service. This is called automatically during initialization but can be re-invoked if needed.
python

# Connection is created during initialization
client = AzureStorageTableClient(account_name="myaccount", access_key="mykey")

# Re-create connection if needed
client.create_connection()

2. get_table_names()
Retrieves a list of all table names in the storage account.
python

try:
    tables = client.get_table_names()
    print("Tables in storage account:", tables)
except Exception as e:
    print(f"Error: {e}")

Output (example):

Tables in storage account: ['Customers', 'Orders']

3. create_table(table_name)
Creates a new table in the storage account.
python

try:
    client.create_table(table_name="Customers")
except Exception as e:
    print(f"Error: {e}")

Output:

Table "Customers" successfully created

Notes:
Table names must be alphanumeric, 3-63 characters, and start with a letter.

Raises ValueError for invalid table names.

4. delete_table(table_name)
Deletes an existing table.
python

try:
    client.delete_table(table_name="Customers")
except Exception as e:
    print(f"Error: {e}")

Output:

Table "Customers" successfully deleted

Notes:
Raises ValueError if the table doesn’t exist.

5. update_create_entity(table_name, entity, mode)
Creates or updates entities in a table, supporting batch operations. Entities are dictionaries with at least PartitionKey and RowKey.
python

# Single entity
entity = [
    {
        "PartitionKey": "CustomerGroup1",
        "RowKey": "001",
        "Name": "John Doe",
        "Age": 30
    }
]

try:
    client.update_create_entity(table_name="Customers", entity=entity, mode="merge")
except Exception as e:
    print(f"Error: {e}")

# Batch of entities
entities = [
    {
        "PartitionKey": "CustomerGroup1",
        "RowKey": "002",
        "Name": "Jane Smith",
        "Age": 25
    },
    {
        "PartitionKey": "CustomerGroup1",
        "RowKey": "003",
        "Name": "Bob Johnson",
        "Age": 40
    }
]

try:
    client.update_create_entity(table_name="Customers", entity=entities, mode="replace")
except Exception as e:
    print(f"Error: {e}")

Output:

Created table "Customers"
Entities successfully updated

Notes:
mode="merge": Updates only provided properties.

mode="replace": Replaces the entire entity.

Automatically creates the table if it doesn’t exist.

Supports batches of up to 100 entities (handled by create_entity_batch).

6. delete_entity(table_name, partition_key, row_key)
Deletes a specific entity from a table.
python

try:
    client.delete_entity(
        table_name="Customers",
        partition_key="CustomerGroup1",
        row_key="001"
    )
except Exception as e:
    print(f"Error: {e}")

Output:

Entity successfully deleted

Notes:
Raises ValueError if the table or entity doesn’t exist.

7. select_entity(table_name, query_filter, parameters, select, results_per_page)
Queries entities in a table using a filter string, optionally with parameters, selected columns, and pagination.
Example 1: Using a Raw Query Filter
python

try:
    query_result = client.select_entity(
        table_name="Customers",
        query_filter="Age ge 30"
    )
    for entity in query_result:
        print(entity)
except Exception as e:
    print(f"Error: {e}")

Output (example):

{'PartitionKey': 'CustomerGroup1', 'RowKey': '003', 'Name': 'Bob Johnson', 'Age': 40}

Example 2: Using QueryBuilder
python

from azure_table_client import QueryBuilder, Operator

# Build query
query_filter = (
    QueryBuilder()
    .set_column("Age")
    .set_operator(Operator.GREATERTHANOREQUAL)
    .set_value(30)
    .get_query()
)

try:
    query_result = client.select_entity(
        table_name="Customers",
        query_filter=query_filter
    )
    for entity in query_result:
        print(entity)
except Exception as e:
    print(f"Error: {e}")

Output (example):

{'PartitionKey': 'CustomerGroup1', 'RowKey': '003', 'Name': 'Bob Johnson', 'Age': 40}

Example 3: Using Parameters
python

parameters = {"name": "John Doe"}
try:
    query_result = client.select_entity(
        table_name="Customers",
        query_filter="Name eq @name",
        parameters=parameters,
        select=["PartitionKey", "RowKey", "Name"],
        results_per_page=10
    )
    for entity in query_result:
        print(entity)
except Exception as e:
    print(f"Error: {e}")

Output (example):

{'PartitionKey': 'CustomerGroup1', 'RowKey': '001', 'Name': 'John Doe'}

Notes:
QueryBuilder simplifies query construction with operators like EQUAL, GREATERTHAN, etc.

parameters allows parameterized queries for safety.

select limits returned columns.

results_per_page controls pagination (default: 1000).

Using QueryBuilder
The QueryBuilder class helps construct query strings for select_entity.
Example: Complex Query
python

from azure_table_client import QueryBuilder, Operator

query_filter = (
    QueryBuilder()
    .set_column("LastName")
    .set_operator(Operator.GREATERTHANOREQUAL)
    .set_value("A")
    .set_operator(Operator.AND)
    .set_column("LastName")
    .set_operator(Operator.LESSTHAN)
    .set_value("B")
    .get_query()
)

print(query_filter)  # Output: LastName ge 'A' and LastName lt 'B' 

try:
    query_result = client.select_entity(
        table_name="Customers",
        query_filter=query_filter
    )
    for entity in query_result:
        print(entity)
except Exception as e:
    print(f"Error: {e}")

Example: DateTime Query
python

query_filter = (
    QueryBuilder()
    .set_column("CreatedDate")
    .set_operator(Operator.EQUAL)
    .set_value_datetime("2023-10-01T12:00:00Z")
    .get_query()
)

print(query_filter)  # Output: CreatedDate eq datetime2023-10-01T12:00:00Z 

try:
    query_result = client.select_entity(
        table_name="Customers",
        query_filter=query_filter
    )
    for entity in query_result:
        print(entity)
except Exception as e:
    print(f"Error: {e}")

Error Handling
All methods include robust error handling for:
Invalid inputs (ValueError)

Missing connections (AttributeError)

Azure service errors (HttpResponseError, ServiceRequestError)

Wrap method calls in try-except blocks to handle exceptions gracefully.
Contributing
Contributions are welcome! Please submit issues or pull requests to the GitHub repository.
License
This package is licensed under the MIT License. See the LICENSE file for details.

