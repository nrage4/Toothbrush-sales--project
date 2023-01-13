import json
import pandas as pd
import datetime as dt
import boto3
import sqlalchemy
from sqlalchemy import exc


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket = "mystaticwebsite-4"
    key = "order_data_20221205_1450.csv"
    file = s3.get_object(Bucket=bucket, Key=key)
    df_1 = pd.read_csv(file['Body'])
    df_1["Order Date"] = df_1["Order Date"].astype('datetime64')
    df_1['Order Date'] = df_1['Order Date'].dt.date
    df_1['Dispatched Date'] = df_1['Dispatched Date'].astype('datetime64')
    df_1['Dispatched Date'] = df_1['Dispatched Date'].dt.date
    df_1['Delivery Postcode'] = df_1['Delivery Postcode'].str.replace("%", ' ')
    df_1['Delivery Postcode'] = df_1['Delivery Postcode'].apply(str.upper)
    df_1 = df_1.dropna()
    columns_1 = ['is_first', 'Dispatch Status', 'Delivery Status', 'Delivery Date']
    df_1.drop(columns_1, axis=1, inplace=True)

    db_details = "mysql+pymysql://admin:Yasminzubeyda16.@database-1.cdtnnnzrteid.us-east-1.rds.amazonaws.com:3306/toothbrush_database"
    my_engine = sqlalchemy.create_engine(db_details, echo=True)
    for i in range(len(df_1)):
        try:
            df_1.iloc[i:i + 1].to_sql(name='orders_data', con=engine, if_exists='replace', index='id')
        except exc.IntegrityError:

            pass

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }