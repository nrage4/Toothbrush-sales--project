import sqlalchemy
import pandas as pd
import pymysql
import boto3
import json
from sqlalchemy import exc

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket = "mystaticwebsite-4"
    key = "null_order_data.csv"
    file = s3.get_object(Bucket=bucket, Key=key)
    df_2 = pd.read_csv(file['Body'])

    columns_2 = ['Delivery Status', 'Delivery Date']
    df_2.drop(columns_2, axis=1, inplace=True)
    df_2["Order Date"] = df_2["Order Date"].astype('datetime64')
    df_2['Order Date'] = df_2['Order Date'].dt.date
    df_2['Dispatched Date'] = df_2['Dispatched Date'].astype('datetime64')
    df_2['Dispatched Date'] = df_2['Dispatched Date'].dt.date
    df_2['Delivery Postcode'] = df_2['Delivery Postcode'].str.replace("%", ' ')
    df_2['Delivery Postcode'] = df_2['Delivery Postcode'].apply(str.upper)

    for i in range(len(df_2)):
        try:
            df_2.iloc[i:i + 1].to_sql(name='null_orders', con="mysql+pymysql://admin:Yasminzubeyda16.@database-1.cdtnnnzrteid.us-east-1.rds.amazonaws.com:3306/toothbrush_database", if_exists='append', index='id')
        except exc.IntegrityError:

            pass


        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
    }
