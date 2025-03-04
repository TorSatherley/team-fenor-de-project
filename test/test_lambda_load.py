from src.lambda_load import lambda_handler, get_list_of_parquets, parquet_to_pandas_df, upload_df_to_warehouse


class ConvertParquetsToDataframes:
    # test working funct returns a correct dataframe list from all parquets in latest folder
    # test function isnt collecting items from any other folders
    # test function handles exceptions suitably (client and pandas)

    # ARRANGE
    # mock aws s3 client
    # put some test parquet files and folders into it
    # make a test latest_folder folder path

    # ACT
    # call get_list_of_parquets with mocked s3_client and test latest_folder filepath

    # ASSERT
    # assert funct returns the correct list of dataframes
    pass

class TestUploadDfToWarehouse:
    pass



class TestLambdaHandler:
    pass