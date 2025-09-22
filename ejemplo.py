from get_dataframe import get_dataframe
import great_expectations as gx


# Read data into pandas
sql_query ="""select top 50 *
FROM pos.DailyInventoryHistoryDetails
order by PosDailyInventoryHistoryDetailsKey desc;"""
dataframe = get_dataframe(sql_query)

# Create Data Context
context = gx.get_context()

# Add Data Source
data_source = context.data_sources.add_pandas(name="my_pandas_datasource")

# Add Data Asset
data_asset = data_source.add_dataframe_asset(name="my_dataframe_asset")

# Add batch definition
# Batch definition: A configuration of how a Data Asset should be divided for testing
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    name="my_batch_definition"
)

# Add batch using batch_definition
# Batch: A group of records that validations can be run on
batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

# Things a batch can do
# print(batch.head())
# print(batch.head(fetch_all=True))
# print(batch.columns())

# Create Expectations
row_count_expectation = gx.expectations.ExpectTableRowCountToEqual(
    value=dataframe.shape[0]
)

# Get validation results
validation_results = batch.validate(expect=row_count_expectation)
print(validation_results)
print(validation_results.describe())

# Assessing an expectation
print(validation_results.success)
print(validation_results["success"])

# Shape expectations
row_count_expectation = gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=198560,
    max_value=198570
)
row_count_expectation = gx.expectations.ExpectTableColumnCountToEqual(
    value=dataframe.shape[1]
)
row_count_expectation = gx.expectations.ExpectTableColumnCountToBeBetween(
    min_value=8,
    max_value=10
)

# Column expectations
column_expectation = gx.expectations.ExpectTableColumnsToMatchSet(
    column_set=set(dataframe.columns)
)
column_expectation = gx.expectations.ExpectColumnToExist(
    column="TRL"
)

# View success status of Validation Results
print("success:", validation_results.success)

# View observed value of Validation Results
print("result:", validation_results.result)
