import great_expectations as gx
from get_dataframe import get_dataframe


# Obtener datos desde SQL
sql_query = """select top 50 *
FROM pos.DailyInventoryHistoryDetails
where UPC is not null
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

# Create Expectation Suite
suite = gx.ExpectationSuite(name="my_suite")

# Create Expectations
col_name_expectation = gx.expectations.ExpectColumnToExist(column="UPC")

col_upc_expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
    column="UPC", regex=r"^\d{11,13}$", mostly=0.95
)

col_DolOnHandRetailUSD_expectation = gx.expectations.ExpectColumnMinToBeBetween(
    column="DolOnHandRetailUSD",
    min_value=0,
    max_value=1_000_000
)


# Add Expectations to Suite
suite.add_expectation(expectation=col_name_expectation)
suite.add_expectation(expectation=col_upc_expectation)
suite.add_expectation(expectation=col_DolOnHandRetailUSD_expectation)

# View Suite's Expectations
print("Suite's Expectations:")
print(suite.expectations)
print("==================")

# Validate Suite
validation_results = batch.validate(expect=suite)

# Describe Validation Results
print("==================")
print("Describe Validation Results")
print(validation_results.describe())
