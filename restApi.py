import requests
from arcgis.gis import GIS
from arcgis.features import GeoAccessor
import pandas as pd
import json

# ...existing code...
# set up ArcGIS Online credentials
username = "w.zakir"
password = "Autocad006"
my_gis_org = GIS("https://www.arcgis.com", username, password)
# ...existing code...
# set up ArcGIS Online credentials
# my_gis_org = GIS("holme")

# display my user information
my_gis_org.users.me
print(my_gis_org.users.me.username)
username = "w.zakir"
password = "Autocad006"
my_item = my_gis_org.content.get("2e84fff0b77643a1a4b522c0664369bf")
my_layer = my_item.layers[0]
df = my_layer.query(where="1=1", out_fields=['subject', 'status', 'service_code'], as_df = True)
print(df.head())

# Convert DataFrame to list of dictionaries as this api expects a json object
# This is useful for sending data to an API endpoint that expects a JSON payload
list_of_dictionaries = df.to_dict(orient='records')
print(list_of_dictionaries)

# first feature
first_feature = list_of_dictionaries[0]
print(first_feature)

# post data to an API

# now reshape the data to match the API's expected format
# the data needs to be reformatted to have "name" and "data" as keys
payload = {"name": "New feature", "data": first_feature}

# the data will need to be converted to JSON format
# converting th dictionary to JSON
payload_json = json.dumps(payload)

# post requests send data as a payload rahter than a URL parameter. specify headers to help the REST endpoint understand the data that is being sent
headers = {'Content-Type': 'application/json'}

# sending the post request
response = requests.post(url = "https://api.restful-api.dev/objects", data=payload_json, headers=headers)

response.json()
print(response.status_code)
print(response.json())

