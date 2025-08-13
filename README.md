## Task Execution Logic and Validations

When a `QueuedTask` is dequeued (i.e., `nextTask != null`), the program performs several validations and logical checks before executing the BigQuery job:

### Step-by-Step Validation Breakdown

#### 1. **Log Task Metadata**

* Logs the task ID and raw JSON parameters for traceability.

#### 2. **Parameter Extraction**

* Uses `GetJsonParameters()` to extract:

  * `TableName` (required)
  * `TOP` row limit
  * `DateColumn` and `DateRange`
  * `CustomWhereClauseArrayColumn` and corresponding array
  * `CustomValueColumn` and single filter value

#### 3. **Sanity Checks for Inputs**

* **Table Name Validation**

  * If `tableName == null`, job fails immediately.

* **Where Clause Type Decision Tree**

  * If all where-clause values are null: a full table pull is assumed.
  * If `dateColumn` and `dateRange` exist: a date-sliced pull is triggered.
  * If `customWhereClauseArrayColumn` and array exist: custom `IN` clause pull.
  * If `customValueColumn` and string exist: single-value match.

#### 4. **Input Consistency Validations**

* Invalid configurations trigger hard failures:

  * `dateColumn != null` but `dateRange == null`
  * `customWhereClauseArrayColumn != null` but array is null
  * `customValueColumn != null` but value is null

These checks are meant to **fail fast** for malformed input and avoid partial or incorrect data pulls.

#### 5. **BigQuery Credential Loading**

* Loads `credentials.json` using `GoogleCredential.FromFile()`.
* If credentials cannot be loaded or BigQuery client fails to initialize, execution halts.

#### 6. **Data Extraction Execution**

* Based on detected pull type:

  * `CreateTablesWithoutDateRange()`
  * `CreateTablesWithDateRange()`
  * `CreateTablesWithCustomWhereClause()`
* Each function:

  * Runs the query
  * Saves output as CSV
  * Retries once if result is empty

#### 7. **Task Completion Recording**

* Marks task as `Completed` if no exception was thrown
* Else, logs the failure and marks as `Failed`

---

## Sample Test JSON

Here's are 5 fully valid JSON examples that can be used to enqueue a task:

```json
1.
	{
		"TableName": "client-project-id.database.table_name",
		"TOP": 100,
		"WhereClause": {
			"DateColumnName": {
				"StartDate": "2023-01-01",
				"EndDate": "2025-06-01"
			}
		}
	}

2. 
	{
		"TableName": "client-project-id.database.table_name",
		"WhereClause": {
			"DateColumnName ": {
				"StartDate": "2023-01-01"
			}
		}
	}

3. 
	{
		"TableName": "client-project-id.database.table_name",
		"WhereClause": {
			"ID_Number": ["40524825",40525186,40524821,40524824,40524817,40524820,"40524808",40524823]
		}
	}

4. 
	{
		"TableName": "client-project-id.database.table_name",
		"WhereClause": {
			"VOUCHER_ID": 40524825
		}
	}

5. 
	{
		"TableName": "client-project-id.database.table_name"
	}
```

The program is able to do the following:
#### 1. Pull the top 100 rows of a table that is filtered where a specific column is in a set of dates 
#### 2. Pull all rows where the date of a column is greater than a set starting date
#### 3. Pull all rows where the value of a column is any value in a list of values
#### 4. Pull all rows where one column is equal to a specific value
#### 5. Pull the whole table

## DISCRETION:
### ALL CREDENTIALS SEEN IN THIS ARE 100% FAKE. THIS PROGRAM IS FOR SKILL DEMONSTRATION ONLY AND WILL NOT DO ANYTHING
