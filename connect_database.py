from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
import random
from datetime import datetime, timedelta
import pytz

# Cassandra Astra details
ASTRA_CLIENT_ID = "SZheMmqpZfkKISMzGHgzrqNA"
ASTRA_CLIENT_SECRET = "XOya30OLyLew9HdKgubezUddj77vTHT2x-v2d51TfZ,LbdTvYkZ7,O2oCW8AhWG9h6OexZkZ7PUR0JsPyKHgPE56wC.ffSPwfik0u+1z.FGtx0zl_gY.B3,eLawJZeNe"
ASTRA_SECURE_CONNECT_BUNDLE_PATH = "C:\\ConnectBundle\\secure-connect-agripriceanalysis.zip"
ASTRA_KEYSPACE = "agri_keyspace"
TABLE_NAME = "fruitables"

# Connect to Astra database
cloud_config = {
    'secure_connect_bundle': ASTRA_SECURE_CONNECT_BUNDLE_PATH
}

auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
# -----------
fruits = ['apple', 'banana', 'papaya']

# Update existing rows with timestamp for each fruit every 10 seconds
prev_prices = {}
color = ''
for i in range(10):

    for fruit in fruits:
        # change_in_price = round(random.uniform(-5, 5), 2)  # Random price change
        current_price = round(random.uniform(10, 50), 2)  # Random current price
        
        #-----
        current_utc_time = datetime.utcnow()
        utc_timezone = pytz.timezone('UTC')
        ist_timezone = pytz.timezone('Asia/Kolkata')
        ist_time = utc_timezone.localize(current_utc_time).astimezone(ist_timezone)
        curtime = ist_time.strftime('%Y-%m-%d %H:%M:%S')
        print(curtime)

        if fruit not in prev_prices:
            change_in_price = str(0.00) + "% "
            prev_prices[fruit] = current_price
            color = 'Gray'
        else:
            x = ((current_price - prev_prices[fruit])/ prev_prices[fruit]) * 100
            change_in_price = round(x, 2)
            if change_in_price > 0:
                color = 'red'
            elif change_in_price < 0:
                color = 'green'
            else:
                color = 'Gray'
            change_in_price  = str(change_in_price) + ' %'

        query = f"INSERT INTO {ASTRA_KEYSPACE}.{TABLE_NAME} (name, time, price_change, current_price, prev_price, color) VALUES ('{fruit}', '{curtime}', '{change_in_price}', {current_price}, {prev_prices[fruit]}, '{color}')"
        
        session.execute(query)

        prev_prices[fruit] = current_price

    time.sleep(10)  # Insert data every 10 seconds

# ten_seconds_ago = datetime.now() - timedelta(seconds=10)

# rows = session.execute("""
#     SELECT * FROM newSampleTable
#     WHERE timeIs > %s
# """, [ten_seconds_ago])

# for row in rows:
#     print(row)

#---------------------------------------------

# from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider
# import time
# import random
# from datetime import datetime, timedelta
# import pytz

# # Cassandra Astra details
# ASTRA_CLIENT_ID = "SZheMmqpZfkKISMzGHgzrqNA"
# ASTRA_CLIENT_SECRET = "XOya30OLyLew9HdKgubezUddj77vTHT2x-v2d51TfZ,LbdTvYkZ7,O2oCW8AhWG9h6OexZkZ7PUR0JsPyKHgPE56wC.ffSPwfik0u+1z.FGtx0zl_gY.B3,eLawJZeNe"
# ASTRA_SECURE_CONNECT_BUNDLE_PATH = "C:\\ConnectBundle\\secure-connect-agripriceanalysis.zip"
# ASTRA_KEYSPACE = "agri_keyspace"
# TABLE_NAME = "fruitables"

# # Connect to Astra database
# cloud_config = {
#     'secure_connect_bundle': ASTRA_SECURE_CONNECT_BUNDLE_PATH
# }

# auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
# cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
# session = cluster.connect()
# # -----------
# fruits = ['apple', 'banana', 'papaya']

# # Update existing rows with timestamp for each fruit every 10 seconds
# prev_prices = {}
# color = 'Gray'
# for i in range(10):

#     for fruit in fruits:
#         # change_in_price = round(random.uniform(-5, 5), 2)  # Random price change
#         current_price = round(random.uniform(10, 50), 2)  # Random current price

#         if fruit not in prev_prices:
#             change_in_price = str(0.00) + ' %'
#             prev_prices[fruit] = current_price
#             color = 'Gray'
#         else:
#             x = ((current_price - prev_prices[fruit])/ prev_prices[fruit]) * 100
#             change_in_price = round(x, 2)
#             if change_in_price > 0:
#                 color = 'red'
#             elif change_in_price < 0:
#                 color = 'green'
#             else:
#                 color = 'Gray'
#             change_in_price  = str(change_in_price) + ' %'
        
#         current_utc_time = datetime.utcnow()
#         utc_timezone = pytz.timezone('UTC')
#         ist_timezone = pytz.timezone('Asia/Kolkata')
#         ist_time = utc_timezone.localize(current_utc_time).astimezone(ist_timezone)
#         cur_time = ist_time.strftime('%Y-%m-%d %H:%M:%S')

#         # ist_time = utc_timezone.localize(current_utc_time).astimezone(ist_timezone)
#         # cur_time = ist_time.strftime('%Y-%m-%d %H:%M:%S')

#         query = f"INSERT INTO {ASTRA_KEYSPACE}.{TABLE_NAME} (name, time, price_change, current_price, prev_price, color) VALUES ('{fruit}', '{cur_time}', '{change_in_price}', {current_price}, {prev_prices[fruit]}, '{color}')"
        
#         session.execute(query)

#         prev_prices[fruit] = current_price

#     time.sleep(10)  # Insert data every 10 seconds



    # current_time = int(time.time() * 1000)  # Current timestamp in milliseconds
    # for fruit in fruits:
    #     change_in_price = round(random.uniform(-5, 5), 2)  # Random price change
    #     current_price = round(random.uniform(10, 50), 2)  # Random current price

    #     query = f"UPDATE {ASTRA_KEYSPACE}.{TABLE_NAME} SET change_in_price = {change_in_price}, current_price = {current_price}, time = {current_time} WHERE name = '{fruit}'"
    #     session.execute(query)

    # time.sleep(10)  # Update data every 10 seconds

# --------
# for i in range(20):
#     columns = 'name, time, change_in_price, current_price'
#     data = f"{random.randint(1,100)}, 20.4, 34.8"

#     query = f"INSERT INTO {ASTRA_KEYSPACE}.{TABLE_NAME} ({columns}) VALUES ({data})"
#     session.execute(query)

#     time.sleep(5)
# Prepare your insert query
# insert_query = session.prepare(f"INSERT INTO {ASTRA_KEYSPACE}.{TABLE_NAME} (id, change, price) VALUES (1, 2.3, 5.6)")

# data_to_insert = (2, 4.3, 6.7)  # Replace with your data

    # Execute the insert query
# session.execute(insert_query, data_to_insert)

# -------------------
# Continuously insert data
# while True:
#     # Generate your data or fetch it from somewhere
#     data_to_insert = (2, 4.3, 6.7)  # Replace with your data

#     # Execute the insert query
#     session.execute(insert_query, data_to_insert)
    
#     # Adjust the sleep time as needed (e.g., delay between insertions)
#     time.sleep(10)  # 1 second delay between insertions
