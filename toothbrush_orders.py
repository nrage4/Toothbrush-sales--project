import pandas as pd
import datetime as dt
import pymysql
import sqlalchemy
from sqlalchemy import exc
#open postcode df
df = pd.read_csv("C:/Users/NasraRage/Documents/generate_toothbrush_data/open_postcode_geo.csv")
columns = [ 'easting','positional_quality_indicator', 'northing','postcode', 'postcode_no_space', 'postcode_fixed_width_seven', 'postcode_fixed_width_eight','postcode_area', 'postcode_district', 'postcode_sector' , 'outcode', 'incode']
df.drop(columns, axis=1, inplace =True)
df['latitude'] = df['latitude'].replace('\\N', 0.0)
df['longitude'] = df['longitude'].replace('\\N', 0.0)
df['latitude'] = df['latitude'].astype(float)
df['longitude'] = df['longitude'].astype(float)



# order data
df_1 = pd.read_csv("C:/Users/NasraRage/Documents/generate_toothbrush_data/order_data_20221205_1450.csv", parse_dates = ['Order Date', 'Dispatched Date', 'Delivery Date' ])

df_1['Delivery Postcode'] = df_1['Delivery Postcode'].str.replace("%",' ')

df_1 = df_1.dropna()

columns_1 = [ 'is_first', 'Dispatch Status','Delivery Status' , 'Delivery Date']
df_1.drop(columns_1, axis=1, inplace = True)

df_1['Order Date'] = df_1['Order Date'].dt.date
df_1['Dispatched Date'] = df_1['Dispatched Date'].dt.date

df_1['Delivery Postcode'] = df_1['Delivery Postcode'].apply(str.upper)

# null order data
df_2 = pd.read_csv("C:/Users/NasraRage/Documents/generate_toothbrush_data/null_order_data.csv", parse_dates = ['Order Date', 'Dispatched Date', 'Delivery Date' ]) 



columns_2 = [ 'Delivery Status', 'Delivery Date']
df_2.drop(columns_1, axis=1, inplace = True)


df_2['Order Date'] = df_2['Order Date'].dt.date
df_2['Dispatched Date'] = df_2['Dispatched Date'].dt.date



df_2['Delivery Postcode'] = df_2['Delivery Postcode'].str.replace("%",' ')
df_2['Billing Postcode'] = df_2['Billing Postcode'].apply(str.upper)


# maria db
db_details = "mysql+pymysql://admin:Yasminzubeyda16.@database-1.cdtnnnzrteid.us-east-1.rds.amazonaws.com:3306/toothbrush_database"
my_engine = sqlalchemy.create_engine(db_details, echo=True)
#df.to_sql(name='order_data', con=my_engine, if_exists='append', index='id')
#df_1.to_sql(name='order_data', con=my_engine, if_exists='append', index='id')
#df_2.to_sql(name='null_data_orders', con=my_engine, if_exists='append', index='id')


try:
    df.to_sql(name='toothbrush_orders', con=my_engine, if_exists='append', index='id')
    df_1.to_sql(name='order_data', con=my_engine, if_exists='append', index='id')
    df_2.to_sql(name='null_data_orders', con=my_engine, if_exists='append', index='id')
except exc.IntegrityError:
    #Ignore duplicates
    pass


