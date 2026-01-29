using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using CsvHelper;
using ErecoverQueuedTasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using TemplateJobWorkerNode.Utilities;

namespace TemplateJobWorkerNode
{
	internal class Program
	{
		#region Global Variables
		private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
		static readonly NameValueCollection appSettings = System.Configuration.ConfigurationManager.AppSettings;
		private static bool error_occurred = false;
		#endregion
		static void Main(string[] args) 
		{
			if (DisableConsoleQuickEdit.DisableQuickEdit() == false)
			{
				Console.WriteLine("***** Unable to disable QuickEdit Mode - Please manually disable in the window properties;");
			}
			string nodeName = appSettings["MachineNodeName"] ?? "NULL MACHINE NAME";

			Console.WindowHeight = 4;
			Console.WindowWidth = 50;
			int nProcessID;

			using (Process p = Process.GetCurrentProcess())
			{
				nProcessID = p.Id;
				p.PriorityClass = ProcessPriorityClass.BelowNormal;
			}
			Console.Title = appSettings["MachineNodeName"] + " - PID: " + nProcessID.ToString() + " - ATG Big Query Data Pulling";

			int waitTimeSeconds = Convert.ToInt32(appSettings["WaitTimeInSeconds"] ?? "300");
			QueuedTaskType taskType = (QueuedTaskType)Convert.ToInt32(appSettings["TaskTypeId"]);
			string machineName = appSettings["MachineNodeName"] ?? "NULL MACHINE NAME"; // TODO: Set a machine name in the app.config
			string connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["AuditTool"].ConnectionString;
			int clientFamilyId = Convert.ToInt32(appSettings["ClientFamilyId"] ?? "88"); // TODO: If you only want to select tasks for a certain client family, set ClientFamilyId in the app.config

			var taskFactory = new QueuedTaskFactory(QueuedTaskType.BigQuery, machineName, connectionString, clientFamilyId);

			while (true)
			{
				error_occurred = false;
				int queuedTaskID;
				int total_records_found = 0;
				string empty_tables = "";
				System.Configuration.ConfigurationManager.RefreshSection("appSettings");
				bool continueRunning;
				if (bool.TryParse(System.Configuration.ConfigurationManager.AppSettings["ContinueRunning"], out continueRunning))
				{
					if (!continueRunning)
					{
						_logger.Info("Continue value set to false: shutting down.");
						Console.WriteLine("Continue value set to false: shutting down.");
						break;
					}
					else
					{
						_logger.Info("Continue value set to true: getting next indexing job.");
						Console.WriteLine("Continue value set to true: getting next indexing job.");
					}
				}
				else
				{
					_logger.Info("Invalid Continue value: shutting down.");
					Console.WriteLine("Invalid Continue value: shutting down.");
					break;
				}

				DateTime start = DateTime.Now;
				QueuedTask nextTask = null;
				try
				{
					nextTask = taskFactory.GetNextTask();
				}
				catch (Exception exc)
				{
					Console.WriteLine($"Failed to get new task: {exc.Message}");
					_logger.Error(exc, "Failed to get new task:");
				}

				if (nextTask == null)
				{
					Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} No tasks found. Sleeping 5 minutes.");
					_logger.Info("No tasks found. Sleeping 5 minutes.");
					Thread.Sleep(waitTimeSeconds * 1000);
					continue;
				}

				// TODO: Add the class or classes that the json object in the queued task's 
				//var taskParameters = JsonConvert.DeserializeObject<CUSTOM CLASS>(nextTask.TaskParameters);

				// TODO: Put any information you want to in the log for recording.
				Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} Job ID: " + nextTask.QueuedTaskId);

				bool success = false;
				try
				{
					#region Getting Prerequisites
					logAndPrintInfo(_logger, $"Task parameters passed: {nextTask.TaskParameters.ToString()}");
					queuedTaskID = nextTask.QueuedTaskId;
					bool datedTablesToExtract = false;
					bool nonDatedTablesToExtract = false;
					bool tablesWithCustomWhereClausesToExtract = false;
					(
						string tableName, // This is designed to only work for one table per job. This will fail if an array of tables is passed.
						int topNumberOfRows,
						string dateColumn, 
						List<DateTime> dateRange, 
						string customWhereClauseArrayColumn, 
						List<string> customWhereClauseArray, 
						string customValueColumn, 
						string customValue
					) = GetJsonParameters(nextTask.TaskParameters);
					if (!ValidDateRangeAndDateColumn(dateColumn, dateRange)) 
					{
						if (dateColumn != null && dateColumn != null)
						{
							throw new Exception($"Invalid date range parameters were thrown. The date column supplied was {dateColumn}. The date range was {dateRange}");
						}
						if (dateColumn != null && dateColumn == null)
						{
							throw new Exception($"Invalid date range parameters were thrown. The date column supplied was {dateColumn}. The date range was NULL");
						}
						if (dateColumn == null && dateColumn != null)
						{
							throw new Exception($"Invalid date range parameters were thrown. The date column supplied was NULL. The date range was {dateRange}");
						}
						throw new Exception($"Invalid date range parameters were thrown. The date column supplied was NULL. The date range was NULL");
					}
					if (!ValidCustomArrayAndColumn(customWhereClauseArrayColumn, customWhereClauseArray))
					{
						if (customWhereClauseArrayColumn != null && customWhereClauseArray != null)
						{
							throw new Exception($"Invalid array of where clause values passed. Column passed is {customWhereClauseArrayColumn}. The values passed were {customWhereClauseArray.ToString()}");
						}
						if (customWhereClauseArrayColumn != null && customWhereClauseArray == null)
						{
							throw new Exception($"Invalid array of where clause values passed. Column passed is {customWhereClauseArrayColumn}. The values passed were NULL");
						}
						if (customWhereClauseArrayColumn == null && customWhereClauseArray != null)
						{
							throw new Exception($"Invalid array of where clause values passed. Column passed is NULL. The values passed were {customWhereClauseArray.ToString()}");
						}
						throw new Exception($"Invalid array of where clause values passed. Column passed is NULL. The values passed were NULL.");
					}
					#endregion

					#region Checking table names and where clauses for validity
					/*
					 * Checking if the data that will be pulled will be the whole table, a range of dates, a range of items, or a single item.
					 * If the table name is null, an Exception will be thrown and the job will be set as failed.
					 */
					if ( tableName == null )
					{
						logAndPrintError(_logger, "No table name was found. Job failed. Terminating job attempt.");
						throw new Exception("No table name was found. Job failed. Terminating job attempt.");
					}

					if (dateColumn == null && customWhereClauseArrayColumn == null && customValueColumn == null)
					{
						logAndPrintInfo(_logger,"No where clause information found. Full table will be pulled.");
						nonDatedTablesToExtract = true;

					}

					else if(dateColumn != null && dateRange != null)
					{
						logAndPrintInfo(_logger, $"{tableName} will pull data for dates from column {dateColumn} from {dateRange[0]} to {dateRange.Last()} inclusive.");
						datedTablesToExtract = true;
					}

					else if(customWhereClauseArrayColumn != null && customWhereClauseArray != null)
					{
						string customWhereClauseArrayToString = "";
						foreach (string customWhereClause in customWhereClauseArray)
						{
							customWhereClauseArrayToString += $"{customWhereClause}, ";
						}
						logAndPrintInfo(_logger, $"{tableName} will pull data for elements from column {customWhereClauseArrayColumn} for items {customWhereClauseArrayToString}");
						tablesWithCustomWhereClausesToExtract = true;
					}

					else if (customValueColumn != null && customValue != null)
					{
						logAndPrintInfo(_logger, $"{tableName} will pull data for elements where column {customValueColumn} = {customValue}");
						tablesWithCustomWhereClausesToExtract = true;
					}

					/*
					 * Checking if any columns should have had a WHERE clause called, but the parameters of the WHERE clause could not 
					 * be parsed. If any instance does occur, the job will fail and be terminated.
					 */
					if (dateColumn != null && dateRange == null)
					{
						logAndPrintError(_logger, $"Date column found for {dateColumn}, but start and end dates were not properly set and are defaulted to null.");
						throw new Exception("Date column found, but start and end dates were not properly set.");
					}

					else if (customWhereClauseArrayColumn != null && customWhereClauseArray == null)
					{
						logAndPrintError(_logger, $"WHERE clause column was found for {customWhereClauseArrayColumn}, but the corresponding array could not be parsed.");
						throw new Exception("WHERE clause column was found, but the corresponding array could not be parsed.");
					}

					else if (customValueColumn != null && customValue == null)
					{
						logAndPrintError(_logger, $"WHERE clause column was found for {customValueColumn}, but the corresponding string could not be parsed.");
						throw new Exception("WHERE clause column was found, but the corresponding string could not be parsed.");
					}
					#endregion

					#region BigQuery Credentials
					/* Setting the BigQuery Credentials provided by Schnucks directly from file.
					 * A copy of these credentials were added in App.config for reference but are not directly used.
					 * THIS PROGRAM WILL IMMEDIATELY STOP IF schnucks-atg-sa-f87620578592.json IS NOT FOUND*/
					string credentialsPath = appSettings["credentialsPath"];
					GoogleCredential credential;
					BigQueryClient client;
					try
					{
						credential = GoogleCredential.FromFile(credentialsPath);
						client = BigQueryClient.Create("schnucks-datalake-prod", credential);
					}
					catch (Exception e)
					{
						logAndPrintError(_logger, $"BigQuery Credentials could not be verified.\nError: {e.ToString()}");
						error_occurred = true;
						throw new Exception($"BigQuery Credentials could not be verified.\nError: {e.ToString()}");
					}
					#endregion

					#region Data Extraction
					// All tables that do not have a dated column will be extracted entirely first due to being much smaller.
					if (nonDatedTablesToExtract)
					{
						(total_records_found,empty_tables) = CreateTablesWithoutDateRange(tableName, topNumberOfRows, client, appSettings, _logger, queuedTaskID);
						/* If all data has been successfully extracted, and saved with no errors of any kind, send a success email
						 * out to the email designated in App.config, log the success and display the success to the console. */
						SaveATGStatsJson(queuedTaskID.ToString(), empty_tables.ToString());
						logAndPrintInfo(_logger, $"Big Query API data for table {tableName} has been successfully extracted.");
						success = true;
					}

					/* All tables with a dated column will now be extracted. Every table for a single day will be extracted
					 * before moving on to the next day.*/
					else if (datedTablesToExtract && dateRange != null)
					{
						(total_records_found, empty_tables) = CreateTablesWithDateRange(dateRange, tableName, dateColumn, topNumberOfRows, client, appSettings, _logger, queuedTaskID);
						/* If all data has been successfully extracted, and saved with no errors of any kind, send a success email
						 * out to the email designated in App.config, log the success and display the success to the console. */
						SaveATGStatsJson(queuedTaskID.ToString(), empty_tables.ToString());
						logAndPrintInfo(_logger, $"Big Query API data for table {tableName} where {dateColumn} on or after {dateRange[0]} and on or before {dateRange.Last()} has been successfully extracted.");
						success = true;
					}

					else if(tablesWithCustomWhereClausesToExtract && customWhereClauseArray != null && customWhereClauseArray.Count > 0)
					{
						(total_records_found, empty_tables) = CreateTablesWithCustomWhereClause(customWhereClauseArray, tableName, topNumberOfRows, customWhereClauseArrayColumn, client, appSettings, _logger, queuedTaskID);
						string allClausesCalled = "";
						foreach (string clause in customWhereClauseArray)
						{
							allClausesCalled += $"{clause},";
						}
						SaveATGStatsJson(queuedTaskID.ToString(), empty_tables.ToString());
						/* If all data has been successfully extracted, and saved with no errors of any kind, send a success email
						 * out to the email designated in App.config, log the success and display the success to the console. */
						logAndPrintInfo(_logger, $"Big Query API data for {tableName} has been successfully extracted where {customWhereClauseArrayColumn} = {allClausesCalled}");
						success = true;
					}

					else if (customValueColumn != null && customValue != null)
					{
						(total_records_found, empty_tables) = CreateTablesWithCustomWhereClause(new List<string> { customValue }, tableName, topNumberOfRows, customValueColumn, client, appSettings, _logger, queuedTaskID);
						/* If all data has been successfully extracted, and saved with no errors of any kind, send a success email
						 * out to the email designated in App.config, log the success and display the success to the console. */
						SaveATGStatsJson(queuedTaskID.ToString(), empty_tables.ToString());
						logAndPrintInfo(_logger, $"Big Query API data for {tableName} has been successfully extracted where {customValueColumn} = {customValue}.");
						success = true;
					}
					if (error_occurred)
					{
						success = false;
					}
					#endregion
				}
				catch (Exception exc)
				{
					_logger.Error(exc, ""); // TODO: Add appropriate error messages or add additional catches for different errors that may be thrown.
				}

				// TODO: You may want to change how success or failure is calculated based on your task.
				try
				{
					if (success)
					{
						taskFactory.MarkTaskComplete(nextTask, QueuedTaskStatus.Completed,outputJson: empty_tables);
					}
					else
					{
						taskFactory.MarkTaskComplete(nextTask, QueuedTaskStatus.Failed, $"Errors generated. Contact developer for review.",empty_tables); // TODO: If you pass a message in here, it will send an email with it.
					}
				}
				catch (Exception exc)
				{
					_logger.Error(exc, "Failed to update task completion:");
				}

				DateTime end = DateTime.Now;
				TimeSpan ts = (end - start);
				Console.WriteLine("Elapsed Time is {0} minutes", ts.TotalMinutes);
				_logger.Info("Process Complete: " + end.ToString());
			}
		}

		#region Miscellaneous Supporting Functions
		static void logAndPrintError(Logger _logger, string message)
		{
			_logger.Error(message);
			//Program.emailError += message;
			//Program.error_occurred = true;
			Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} {message}");
		}

		static void logAndEmailError(Logger _logger, string toEmail, string message)
		{
			_logger.Error(message);
			//Program.emailError += message;
			//Program.error_occurred = true;
			Console.WriteLine(message);
			//EmailUtilities.SendEmail(toEmail, "Error: Big Query Data Extract", message);
		}

		static void logAndPrintInfo(Logger _logger, string message)
		{
			_logger.Info(message);
			Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} {message}");
		}

		/// <summary>
		/// Connect to SQL Server Management Studio server barney, and open the Schnucks_Big_Query_Table_Columns
		/// table. Select the data type value of a specific column for a specific table. 
		/// </summary>
		/// <param name="table">Full table name including project id, dataset, and table id. Must be in form of project.dataset.table</param>
		/// <param name="column">Column name to be identified in the table</param>
		/// <returns>Data type of specified column from specified table in parameters. Will be in all uppercase</returns>
		static string GetColumnType(string table, string column)
		{
			string[] table_parts = table.Split('.');
			string project_id = table_parts[0];
			string dataset_id = table_parts[1];
			string table_id = table_parts[2];
			using (SqlConnection connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["BigQueryColumnPuller"].ConnectionString))
			{
				try
				{
					connection.Open();

					// Example: Execute a simple query
					string query = $"SELECT data_type " +
						$"FROM {appSettings["BigQueryColumnsTable"]} " +
						$"WHERE " +
						$"project_id=\'{project_id}\' AND " +
						$"dataset_id=\'{dataset_id}\' AND " +
						$"table_id=\'{table_id}\' AND " +
						$"column_name=\'{column}\'"
						;
					using (SqlCommand command = new SqlCommand(query, connection))
					{
						using (SqlDataReader reader = command.ExecuteReader())
						{
							if (reader.Read())
							{
								return reader["data_type"].ToString().ToUpper();
							}
							else
							{
								logAndPrintError(_logger, $"Could not find a column named {column} in {table} or {table} does not exist in [sandbox_mp].[dbo].[Schnucks_Big_Query_Table_Columns]");
								connection.Close();
								return null;
							}
						}
					}
				}
				catch (SqlException ex)
				{
					logAndPrintError(_logger,$"SQL Error: {ex.Message}");
					connection.Close();
					return null;
				}
				catch (Exception ex)
				{
					logAndPrintError(_logger, $"General Error: {ex.Message}");
					connection.Close();
					return null;
				}
			}
		}
		#endregion

		#region Getting Query Parameters
		static (string, int, string, List<DateTime>, string, List<string>, string, string) GetJsonParameters(string json)
		{
			QueryObject query = JsonConvert.DeserializeObject<QueryObject>(json);
			string tableName = query.TableName;
			int number_of_rows = GetTopNumberOfRows(query);
			string dateColumn = GetDateColumn(query);
			List<DateTime> dateRange = GetDateRange(query);
			string customWhereClauseArrayColumn = GetCustomWhereClauseArrayColumn(query);
			List<string> values = GetCustomWhereClauseArray(query);
			string customValueColumn = GetCustomWhereClauseStringColumn(query);
			string customValue = GetCustomWhereClauseString(query);
			return (tableName, number_of_rows, dateColumn, dateRange, customWhereClauseArrayColumn, values, customValueColumn, customValue);
		}

		static bool ValidDateRangeAndDateColumn(string dateColumn,List<DateTime> dateRange)
		{
			if ((dateColumn != null)&&(dateRange==null || dateRange[0].Year < 2015))
			{
				return false;
			}
			if (dateColumn == null && dateRange != null)
			{
				return false;
			}
			return true;
		}

		static bool ValidCustomArrayAndColumn(string customArrayColumn, List<string> customArrayValues)
		{
			if ((customArrayColumn != null) && (customArrayValues == null))
			{
				return false;
			}
			if (customArrayColumn == null && customArrayValues != null)
			{
				return false;
			}
			return true;
		}

		static string GetDateColumn(QueryObject query)
		{
			if(query.WhereClause == null)
			{
				return null;
			}
			foreach (var condition in query.WhereClause)
			{
				if (condition.Value.Type == JTokenType.Object)
				{
					return condition.Key.ToString();
				}
			}
			return null;
		}

		static List<DateTime> GetDateRange(QueryObject query)
		{
			if(query.WhereClause == null)
			{
				return null;
			}
			foreach (var condition in query.WhereClause)
			{
				if (condition.Value.Type == JTokenType.Object)
				{
					var dateRange = condition.Value.ToObject<DateRange>();

					// If StartDate is null or empty, default to today's date
					string startDatestr = string.IsNullOrWhiteSpace(dateRange.StartDate)
						? new DateTime(1900,1,1).ToString("yyyy-MM-dd")
						: dateRange.StartDate;
					

					DateTime startDate = GetDateOverride(startDatestr, _logger);
					if (startDate.Year < 2015)
					{
						return null;
					}

					// If EndDate is null or empty, default to today's date
					string endDateStr = string.IsNullOrWhiteSpace(dateRange.EndDate)
						? DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")
						: dateRange.EndDate;

					DateTime endDate = GetDateOverride(endDateStr, _logger);

					return GetAllDatesFromOverrideValues(startDate, endDate, _logger);
				}
			}
			return null;
		}

		static string GetCustomWhereClauseArrayColumn(QueryObject query)
		{
			if (query.WhereClause == null)
			{
				return null;
			}
			foreach (var condition in query.WhereClause)
			{
				if (condition.Value.Type == JTokenType.Array)
				{
					return condition.Key.ToString();
				}
			}
			return null;
		}

		static List<string> GetCustomWhereClauseArray(QueryObject query)
		{
			if(query.WhereClause == null)
			{
				return null;
			}
			foreach (var condition in query.WhereClause)
			{
				if (condition.Value.Type == JTokenType.Array)
				{
					// List of custom strings
					return condition.Value.ToObject<List<string>>();
				}
			}
			return null;
		}

		static string GetCustomWhereClauseString(QueryObject query)
		{
			if(query.WhereClause == null) 
			{
				return null; 
			}
			foreach (var condition in query.WhereClause)
			{
				if ((condition.Value != null || condition.Value.Type != JTokenType.Null)&&(condition.Value.Type != JTokenType.Array))
				{
					// String Where clause condition
					return condition.Value.ToString();
				}
			}
			return null;
		}

		static string GetCustomWhereClauseStringColumn(QueryObject query)
		{
			if(query.WhereClause == null)
			{
				return null;
			}
			foreach (var condition in query.WhereClause)
			{
				if ((condition.Value != null || condition.Value.Type != JTokenType.Null) && (condition.Value.Type != JTokenType.Array))
				{
					// String Where clause condition
					return condition.Key;
				}
			}
			return null;
		}

		static DateTime GetDateOverride(string variableDate, Logger _logger)
		{
			try
			{
				Console.WriteLine(variableDate);
				return DateTime.Parse(variableDate);
			}
			catch
			{
				logAndPrintInfo(_logger, $"Could not parse {variableDate} to DateTime from yyyy-mm-dd. Attempting to parse from yyyymmdd format.");
			}
			try
			{
				return DateTime.Parse(DateTime.ParseExact(
						s: variableDate,
						format: "yyyyMMdd",
						provider: CultureInfo.InvariantCulture
					).ToString("yyyy-MM-dd"));
			}
			catch
			{
				logAndPrintError(_logger, $"No override date found for {variableDate}.");
				return new DateTime(1900, 1, 1);
			}
		}

		static List<DateTime> GetAllDatesFromOverrideValues(DateTime start, DateTime end, Logger _logger)
		{
			/* Create a list of all dates that are between the start and end date. This function will return null
			 * and issue a warning to the logger and to the console if two valid dates are found but the 
			 * start date is later than the end date. It will not assume you had it backwards. If either 
			 * the start date or end date are null, the logger will inform that no overriding dates were found
			 * and will return null. */
			if (start > end)
			{
				logAndPrintError(_logger, $"Starting date and ending date found, but start date must be before the ending date. \nExample: \n\tstartDate=2024/01/01\n\tendDate=2024/12/31\nActual Dates Passed: Start={start} , End={end}");
				return null;
			}

			List<DateTime> dates = new List<DateTime>();

			for (DateTime date = start; date <= end; date = date.AddDays(1))
			{
				dates.Add(date);
			}

			return dates;
		}
		
		static int GetTopNumberOfRows(QueryObject query)
		{
			string num_rows_value = string.IsNullOrWhiteSpace(query.TOP)
				? "-1"
				: query.TOP;
			int num_rows;
			if (int.TryParse(num_rows_value, out num_rows))
			{
				return num_rows;
			}
			return -1;
		}
		#endregion

		#region Setting Data Destination
		/// <summary>
		/// Loads the absolute file path name from the settings. If the proper setting can not be found, 
		/// an error is logged and null is returned and any data found for this file will not be saved.
		/// </summary>
		/// <param name="settings"></param>
		/// <param name="_logger"></param>
		/// <returns>String of the absolute file path to save csvs to.</returns>
		static string setFinalFilePath(NameValueCollection settings, Logger _logger)
		{
			try
			{
				return settings["FinalFilePath"];
			}
			catch (Exception e)
			{
				logAndPrintError(_logger, $"Testing file path could not be found. Data will not be saved. Error thrown: {e.ToString()}");
				return null;
			}
		}
		#endregion

		#region Data Extraction
		/// <summary>
		/// Call big query table with a date where clause on a column.. These where clauses can either include
		/// a single start date, or a start and end date. If only a start date is passed, then all dates from the 
		/// start date to the current date are assumed. If a start date and end date are passed, then a list of
		/// all dates from start to end INCLUSIVE are made. The program will create one single csv file per 
		/// where clause found. The name of the csv created includes the name of the table, and the current value 
		/// of the where clause. If the current where clause being parsed
		/// is also the last where clause that will be pulled, then the csv filename will be prefixed with EOT 
		/// to indicate the job is ending with this file. If a query pulled is ever found to have 0 rows, then 
		/// when the job ends, the clauses with 0 rows will be attempted a second time to ensure there was not just 
		/// a big query issue and there truly are 0 records to be pulled. Any table with 0 records is also logged 
		/// in case further investigation is needed.
		/// </summary>
		/// <param name="dates"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <param name="topNumberOfRows"></param>
		/// <param name="client"></param>
		/// <param name="settings"></param>
		/// <param name="_logger"></param>
		/// <param name="queuedTaskID"></param>
		/// <param name="first_attempt"></param>
		/// <returns>Tuple where the first element is an integer indicating the total number of records found
		/// over the course of the entire job. The second element is a full output JSON that will be populated to 
		/// inform the user of everything that was pulled and how many records were pulled for each csv file.</returns>
		static (int,string) CreateTablesWithDateRange(List<DateTime> dates, string table, string column, int topNumberOfRows, BigQueryClient client, NameValueCollection settings, Logger _logger, int queuedTaskID, bool first_attempt = true)
		{
			int total_records_found = 0;
			// This function is extracts all data from a list of tables that have date ranges.
			StringBuilder end_json = new StringBuilder("[");
			string query = null;
			string columnType = GetColumnType(table, column);
			List<List<object>> empty_tables = new List<List<object>>();
			foreach (DateTime currentDate in dates) // Loop through all tables for a single day first, then move to the next day
			{
				logAndPrintInfo(_logger, $"Extracting {table} for column {column} on {currentDate:yyyy-MM-dd}");
				try
				{
					if (topNumberOfRows > 0)
					{
						if (string.Equals(columnType, "NUMERIC", StringComparison.OrdinalIgnoreCase))
						{
							query = $"SELECT * FROM `{table}` WHERE {column} >= {currentDate:yyyyMMdd} AND {column} < {currentDate.AddDays(1.0):yyyyMMdd} LIMIT {topNumberOfRows}";
						}
						else
						{
							query = $"SELECT * FROM `{table}` WHERE {column} >= '{currentDate:yyyy-MM-dd}' AND {column} < '{currentDate.AddDays(1.0):yyyy-MM-dd}' LIMIT {topNumberOfRows}";
						}
					}
					else
					{
						if (string.Equals(columnType, "NUMERIC", StringComparison.OrdinalIgnoreCase))
						{
							query = $"SELECT * FROM `{table}` WHERE {column} >= {currentDate:yyyyMMdd} AND {column} < {currentDate.AddDays(1.0):yyyyMMdd}";
						}
						else
						{
							query = $"SELECT * FROM `{table}` WHERE {column} >= '{currentDate:yyyy-MM-dd}' AND {column} < '{currentDate.AddDays(1.0):yyyy-MM-dd}'";
						}
					}
				}
				catch (Exception e)
				{
					error_occurred = true;
					logAndPrintError(_logger, $"Data failed to load for {table}. Error Thrown: {e.ToString()}");
				}
				if (query != null)
				{
					try
					{
						var results = client.ExecuteQuery(query, null); // Making the actual BigQuery API call
						DataTable dt = ConvertToDataTable(results); // Convert the results to a data table
						total_records_found += dt.Rows.Count;
						if (dates.IndexOf(currentDate) == 0)
						{
							end_json.Append($"{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{currentDate:yyyyMMdd}.csv\", \"RecCount\": \"{dt.Rows.Count}\"}}");
						}
						else
						{
							end_json.Append($",\n{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{currentDate:yyyyMMdd}.csv\", \"RecCount\": \"{dt.Rows.Count}\"}}");
						}
						string path = setFinalFilePath(settings, _logger); // Setting where to save the data table
						if (path == null)
						{
							logAndPrintError(_logger, $"Data for {table} failed to save to {path}.");
							error_occurred = true;
						}
						else
						{
							/* 
								* If the table is empty, it will check if this is the first or second attempt at extracting
								* the table. If this is the first attempt, the program will save this table for the end and 
								* attempt to extract it again. If this is the second attempt at extraction, then an error is
								* thrown into the log, and sent to the console and will be added to the error email at the end
								* of the program.
							*/
							if (dt.Rows.Count == 0)
							{
								if (first_attempt)
								{
									empty_tables.Add(new List<object> { new List<DateTime> { currentDate }, table, column, topNumberOfRows, client, settings, _logger });
								}
								else
								{
									logAndPrintError(_logger, $"Extraction for {table} on {currentDate} came back with 0 rows on two straight attempts.");
									return (total_records_found, $"{end_json.ToString()}]");
								}
							}
							else if ((dt.Rows.Count == 0 && !first_attempt) || (dt.Rows.Count > 0))
							{
								SaveToCsv(dt, $"{path}/{queuedTaskID.ToString()}_{table}_{currentDate:yyyyMMdd}.csv");
							}
						}
					}
					// This will fail if an issue occurs while running the query
					catch (Exception e)
					{
						logAndPrintError(_logger, $"Data failed to load for {table}. Error Thrown: {e.ToString()}");
						error_occurred = true;
					}
				}
				query = null; // Reset query to be null before doing the next table
			}

			/*
			 * If at any point, a table extracted came up empty, this is when the program will attempt to extract
			 * it a second time. If the extraction comes up empty a second time, that is when the error will be 
			 * logged and emailed out.
			 */
			if (empty_tables.Count > 0)
			{
				foreach (List<object> table_set in empty_tables)
				{
					if (
						(table_set.Count == 7) &&
						(table_set[0] is List<DateTime> addedDate) &&
						(table_set[1] is string table_name) &&
						(table_set[2] is string datedColumnName) &&
						(table_set[3] is int top_number_of_rows) &&
						(table_set[4] is BigQueryClient client_name) &&
						(table_set[5] is NameValueCollection setting_collection) &&
						(table_set[6] is Logger logging_tool)
					)
					{
						int change_in_records_found;
						string end_json_second_attempt;
						(change_in_records_found, end_json_second_attempt) = CreateTablesWithDateRange(addedDate, table_name, datedColumnName, top_number_of_rows, client_name, setting_collection, logging_tool, queuedTaskID, false);
						total_records_found += change_in_records_found;
						if(change_in_records_found > 0)
						{
							end_json.Append($",{end_json_second_attempt.Replace("[","").Replace("]", "")}");
						}
					}
				}
			}
			return (total_records_found,$"{end_json.ToString()}]");
		}

		/// <summary>
		/// Call big query table with a custom where clause on a column that is not a date. These where clauses
		/// can be of a single value or a list of values. The program will create one single csv file per 
		/// where clause found. The name of the csv created includes the name of the table, the column of the 
		/// where clause, and the current value of the where clause. If the current where clause being parsed
		/// is also the last where clause that will be pulled, then the csv filename will be prefixed with EOT 
		/// to indicate the job is ending with this file. If a query pulled is ever found to have 0 rows, then 
		/// when the job ends, the clauses with 0 rows will be attempted a second time to ensure there was not just 
		/// a big query issue and there truly are 0 records to be pulled. Any table with 0 records is also logged 
		/// in case further investigation is needed.
		/// </summary>
		/// <param name="clauses"></param>
		/// <param name="table"></param>
		/// <param name="topNumberOfRows"></param>
		/// <param name="column"></param>
		/// <param name="client"></param>
		/// <param name="settings"></param>
		/// <param name="_logger"></param>
		/// <param name="queuedTaskID"></param>
		/// <param name="first_attempt"></param>
		/// <returns>Tuple where the first element is an integer indicating the total number of records found
		/// over the course of the entire job. The second element is a full output JSON that will be populated to 
		/// inform the user of everything that was pulled and how many records were pulled for each csv file.</returns>
		static (int,string) CreateTablesWithCustomWhereClause(List<string> clauses, string table, int topNumberOfRows, string column, BigQueryClient client, NameValueCollection settings, Logger _logger, int queuedTaskID, bool first_attempt = true)
		{
			// This function is extracts all data from a list of tables that have date ranges.
			string query = null;
			int total_records_found = 0;
			int change_in_records_found;
			StringBuilder end_json = new StringBuilder("[");
			List<List<object>> empty_tables = new List<List<object>>();
			foreach (object currentClause in clauses)
			{
				if (clauses.Last().Equals(currentClause))
				{
					(empty_tables, change_in_records_found) = RunQuery(table, currentClause.ToString(), column, topNumberOfRows, query, empty_tables, client, settings, _logger, queuedTaskID, true, first_attempt);
				}
				else
				{
					(empty_tables, change_in_records_found) = RunQuery(table, currentClause.ToString(), column, topNumberOfRows, query, empty_tables, client, settings, _logger, queuedTaskID, false, first_attempt);
				}
				total_records_found += change_in_records_found;
				if (clauses.IndexOf(currentClause.ToString()) == 0)
				{
					if (clauses.Last().Equals(currentClause)&&first_attempt)
					{
						end_json.Append($"{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{column}_{currentClause}.csv\", \"RecCount\": \"{change_in_records_found}\"}}");
					}
					else
					{
						end_json.Append($"{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{column}_{currentClause}.csv\", \"RecCount\": \"{change_in_records_found}\"}}");
					}
				}
				else
				{
					if (clauses.Last().Equals(currentClause) && first_attempt)
					{
						end_json.Append($",\n{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{column}_{currentClause}.csv\", \"RecCount\": \"{change_in_records_found}\"}}");
					}
					else
					{
						end_json.Append($",\n{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{column}_{currentClause}.csv\", \"RecCount\": \"{change_in_records_found}\"}}");
					}
				}
			}

			/*
			 * If at any point, a table extracted came up empty, this is when the program will attempt to extract
			 * it a second time. If the extraction comes up empty a second time, that is when the error will be 
			 * logged and emailed out.
			 */
			if (empty_tables.Count > 0)
			{
				foreach (List<object> table_set in empty_tables)
				{
					if (
						(table_set.Count == 7) &&
						(table_set[0] is List<string> currentClauses) &&
						(table_set[1] is string table_name) &&
						(table_set[2] is int top_number_of_rows) &&
						(table_set[3] is string column_name) &&
						(table_set[4] is BigQueryClient client_name) &&
						(table_set[5] is NameValueCollection setting_collection) &&
						(table_set[6] is Logger logging_tool)
					)
					{
						string change_in_empty_to_string;
						(change_in_records_found,change_in_empty_to_string) = CreateTablesWithCustomWhereClause(currentClauses, table_name, top_number_of_rows, column_name, client_name, setting_collection, logging_tool, queuedTaskID, false);
						total_records_found += change_in_records_found;
					}
				}
			}
			return (total_records_found, $"{end_json.ToString()}]");
		}

		/// <summary>
		/// Helper function to run the query for CreateTablesWithCustomWhereClause and save the results to csv.
		/// Takes the called table and selects data from big query based on the clause provided on a specific 
		/// column. The function catches and logs any and all errors that may come up during execution and 
		/// makes a proper return without throwing exceptions and terminating the program. If the clause being
		/// passed is the last clause is a list of clauses, the csv saved has a prefix of EOT to inform users 
		/// that the csv being saved is the last one and the job is complete.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="currentClause"></param>
		/// <param name="column"></param>
		/// <param name="topNumberOfRows"></param>
		/// <param name="query"></param>
		/// <param name="empty_tables"></param>
		/// <param name="client"></param>
		/// <param name="settings"></param>
		/// <param name="_logger"></param>
		/// <param name="queuedTaskID"></param>
		/// <param name="lastElement"></param>
		/// <param name="first_attempt"></param>
		/// <returns>Tuple where the first element is a List of tables that came back with 0 records found and the 
		/// number of records found when the current query was run</returns>
		static (List<List<object>>,int) RunQuery(string table, string currentClause, string column, int topNumberOfRows, string query, List<List<object>> empty_tables, BigQueryClient client, NameValueCollection settings, Logger _logger, int queuedTaskID, bool lastElement, bool first_attempt)
		{
			logAndPrintInfo(_logger, $"Extracting {table} where {column} is {currentClause}");
			int total_records_found = 0;
			try
			{
				query = $"SELECT * FROM `{table}` WHERE {column} = {currentClause}";
				if (topNumberOfRows > 0)
				{
					query = $"SELECT * FROM `{table}` WHERE {column} = '{currentClause}' LIMIT {topNumberOfRows}";
				}
				else
				{
					query = $"SELECT * FROM `{table}` WHERE {column} = '{currentClause}'";
				}
			}
			catch (Exception e)
			{
				logAndPrintError(_logger, $"Data failed to load for {table}. Error Thrown: {e.ToString()}");
				error_occurred = true;
			}
			if (query != null)
			{
				try
				{
					var results = client.ExecuteQuery(query, null); // Making the actual BigQuery API call
					string path = setFinalFilePath(settings, _logger); // Setting where to save the data table
					string filePath = null;
					if (lastElement && first_attempt)
					{
						filePath = $"{path}/{queuedTaskID.ToString()}_{table}_{column}_{currentClause}.csv";
					}
					else
					{
						filePath = $"{path}/{queuedTaskID.ToString()}_{table}_{column}_{currentClause}.csv";
					}
					DataTable dt = ConvertToDataTable(results);
					total_records_found += dt.Rows.Count;
					if (path == null)
					{
						logAndPrintError(_logger, $"Data for {table} failed to save to {path}.");
					}
					else
					{
						

						/* 
							* If the table is empty, it will check if this is the first or second attempt at extracting
							* the table. If this is the first attempt, the program will save this table for the end and 
							* attempt to extract it again. If this is the second attempt at extraction, then an error is
							* thrown into the log, and sent to the console and will be added to the error email at the end
							* of the program.
						*/
						if (dt.Rows.Count == 0)
						{
							if (first_attempt)
							{
								empty_tables.Add(new List<object> { new List<string> { currentClause }, table, topNumberOfRows, column, client, settings, _logger });
							}
							else
							{
								logAndPrintError(_logger, $"Extraction for {table} came back with 0 rows on two straight attempts.");
								return (empty_tables,total_records_found);
							}
						}
						else if ((dt.Rows.Count == 0 && !first_attempt) || (dt.Rows.Count > 0))
						{
							// Saving the table to the test path as a CSV file
							SaveToCsv(dt, filePath);
						}
					}
				}
				// This will fail if an issue occurs while running the query
				catch (Exception e)
				{
					logAndPrintError(_logger, $"Data failed to load for {table}. Error Thrown: {e.ToString()}");
				}
			}
			query = null; // Reset query to be null before doing the next table
			return (empty_tables, total_records_found);
		}

		/// <summary>
		/// Function to select everything from a specified big query table and pull everything. The only permitted clause
		/// for this function is TOP to limit the number of rows pulled.
		/// Example: SELECT TOP 100 * FROM table
		/// This will always be a select *, but a TOP clause is optionally permitted to be passed.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="topNumberOfRows"></param>
		/// <param name="client"></param>
		/// <param name="settings"></param>
		/// <param name="_logger"></param>
		/// <param name="queuedTaskID"></param>
		/// <param name="first_attempt"></param>
		/// <returns>Tuple where the first element is an integer indicating the total number of records found 
		/// when pulling data from the table. The second element is the output JSON string created by running
		/// this query. Since this function is limited to just one table per execution, the CSV path always 
		/// prefixes with EOT to indicate to users the job has concluded.</returns>
		static (int,string) CreateTablesWithoutDateRange(string table, int topNumberOfRows, BigQueryClient client, NameValueCollection settings, Logger _logger, int queuedTaskID, bool first_attempt = true)
		{
			List<List<object>> empty_tables = new List<List<object>>();
			StringBuilder end_json = new StringBuilder("[");
			int total_records_found = 0;
			try
			{
				logAndPrintInfo(_logger, $"Extracting {table}");
				string query = null;
				if(topNumberOfRows > 0)
				{
					query = $"SELECT * FROM `{table}` LIMIT {topNumberOfRows}";
				}
				else
				{
					query = $"SELECT * FROM `{table}`";
				}
				
				//Console.WriteLine(query);
				var results = client.ExecuteQuery(query, null);
				DataTable dt = ConvertToDataTable(results);
				total_records_found += dt.Rows.Count;
				end_json.Append($"{{\"FileName\": \"{queuedTaskID.ToString()}_{table}_{DateTime.Today:yyyyMMdd}.csv\", \"RecCount\": \"{total_records_found}\"}}");
				
				/* 
				* If the table is empty, it will check if this is the first or second attempt at extracting
				* the table. If this is the first attempt, the program will save this table for the end and 
				* attempt to extract it again. If this is the second attempt at extraction, then an error is
				* thrown into the log, and sent to the console and will be added to the error email at the end
				* of the program.
				*/
				if (dt.Rows.Count == 0)
				{
					if (first_attempt)
					{
						empty_tables.Add(new List<object> { table, topNumberOfRows, client, settings, _logger });
					}
					else
					{
						logAndPrintError(_logger, $"Extraction for {table} came back with 0 rows on two separate attempts.");
						return (total_records_found, $"{end_json.ToString()}]");
					}
				}
				else if((dt.Rows.Count == 0 && !first_attempt)||(dt.Rows.Count > 0)) 
				{
					SaveToCsv(dt, $"{setFinalFilePath(settings, _logger)}/{queuedTaskID.ToString()}_{table}_{DateTime.Today:yyyyMMdd}.csv");
				}
			}
			/* 
				* Check to make sure table names are spelled correctly in App.config. This should only fail 
				* if there is a misspelling in the names of the tables being extracted in the query
				*/
			catch (Exception e)
			{
				logAndPrintError(_logger, $"Data failed to load for {table}. Error Thrown: {e.ToString()}");
				error_occurred = true;
			}

			/*
			 * If at any point, a table extracted came up empty, this is when the program will attempt to extract
			 * it a second time. If the extraction comes up empty a second time, that is when the error will be 
			 * logged and emailed out.
			 */
			if (empty_tables.Count > 0)
			{
				foreach (List<object> table_set in empty_tables)
				{
					if (
						(table_set.Count == 5) &&
						(table_set[0] is string table_name) &&
						(table_set[1] is int top_number_of_rows) &&
						(table_set[2] is BigQueryClient client_name) &&
						(table_set[3] is NameValueCollection setting_collection) &&
						(table_set[4] is Logger logging_tool)
					)
					{
						int change_in_records_found;
						string end_json_second_attempt = "";
						(change_in_records_found, end_json_second_attempt) = CreateTablesWithoutDateRange(table_name, top_number_of_rows, client_name, setting_collection, logging_tool, queuedTaskID, false);
						total_records_found += change_in_records_found;
						if(change_in_records_found > 0)
						{
							end_json.Append($",{end_json_second_attempt}");
						}
					}
				}
			}
			return (total_records_found, $"{end_json.ToString()}]");
		}
		#endregion

		#region Save Results
		static DataTable ConvertToDataTable(BigQueryResults results)
		{
			DataTable dt = new DataTable();
			foreach (var field in results.Schema.Fields)
			{
				dt.Columns.Add(field.Name, typeof(string));
			}

			foreach (var row in results)
			{
				DataRow dataRow = dt.NewRow();
				foreach (var field in results.Schema.Fields)
				{
					dataRow[field.Name] = row[field.Name]?.ToString() ?? "";
				}
				dt.Rows.Add(dataRow);
			}
			return dt;
		}

		static void SaveToCsv(DataTable dataTable, string filePath)
		{
			Directory.CreateDirectory(Path.GetDirectoryName(filePath));

			using (var writer = new StreamWriter(filePath))
			using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
			{
				foreach (DataColumn column in dataTable.Columns)
				{
					csv.WriteField(column.ColumnName);
				}
				csv.NextRecord();

				foreach (DataRow row in dataTable.Rows)
				{
					foreach (DataColumn column in dataTable.Columns)
					{
						csv.WriteField(row[column]);
					}
					csv.NextRecord();
				}
			}
		}

		static void SaveATGStatsJson(string queuedTaskID, string end_json)
		{
			string atgStatsFilePath = $"{appSettings["FinalFilePath"]}/ATGStats{queuedTaskID.ToString()}.json";
			File.WriteAllText(atgStatsFilePath, end_json);
		}
		#endregion

		#region Internal Classes
		class QueryObject
		{
			public string TableName { get; set; }
			public string TOP {  get; set; }
			public Dictionary<string, JToken> WhereClause { get; set; }
		}
		class DateRange
		{
			public string StartDate { get; set; }
			public string EndDate { get; set; }
		}
		#endregion
	}
}