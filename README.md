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
- `azure-data-tables` package
- `utils.py` (included in the repository)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ranoliveira1/azure_table_utils.git
   cd azure_table_utils