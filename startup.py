import os
from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta

print('STARTUP\n')
#-- get, set paths --- 
try:
    base_dir = Path(__file__).resolve().parent
except NameError:
    base_dir = Path(os.getcwd())

for path in ['data', 'data/hourly', 'data/daily']:
    if not os.path.exists(os.path.join(base_dir, path)): 
        os.makedirs(os.path.join(base_dir, path))
        print(f'   *Created /{path} path')
        
#-- read in stations, elements seeking data for ---
stations = pd.read_excel(os.path.join(base_dir, 'dictionaries.xlsx'), sheet_name='stations')
stations['triplet'] = stations['id'].astype(str) + ':' + stations['state'] + ':SNTL'
for col in stations.columns:
    stations[col] = stations[col].astype(str).str.strip()
    
elements = pd.read_excel(os.path.join(base_dir, 'dictionaries.xlsx'), sheet_name='elements')
for col in elements.columns:
    elements[col] = elements[col].astype(str).str.strip()

hourly_element_codes = list(elements.loc[elements.loc[:,'duration']!='Day', 'code'])
daily_element_codes = list(elements.loc[elements.loc[:,'duration']!='Average previous hour', 'code'])

base_url = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"

#----------------------------------------------------
#    HOURLY DATA
#----------------------------------------------------

print('\n\n*** HOURLY DATA *****\n\n')

#-- make API call ---
params = {
    "stationTriplets": list(stations['triplet']),
    "elements": ','.join(hourly_element_codes),
    "duration": 'HOURLY',
    "beginDate": (datetime.today() + timedelta(days = -7)).strftime('%Y-%m-%d'),
    "endDate": '2099-01-01',
    "periodRef": "END",
    "centralTendencyType": "NONE",
    "returnFlags": "false",
    "returnOriginalValues": "false",
    "returnSuspectData": "false"
}

response = requests.get(base_url, params=params)
if response.status_code == 200:
    data = response.json()
    print('API call successful\n\n')
else:
    raise ValueError(f"Hourly data request failed with status code {response.status_code}")
    
#-- unpack response data --- 
for station_item in data:
    station_id = station_item['stationTriplet'].split(':')[0]
    station_data = station_item['data']
    print(str(station_id))
    
    station_df = pd.DataFrame()
    for element_item in station_data:
        element_code = element_item['stationElement']['elementCode']
        values = element_item['values']
        df = pd.DataFrame(values).set_index('date')
        df.columns = [element_code]
        station_df = pd.concat([station_df,df], axis=1)
    station_df = station_df.interpolate(method = 'linear')

    station_path = os.path.join(base_dir, f'hourly_data/{station_id}.parquet')
    if os.path.exists(station_path):
        os.remove(station_path)
        print('   Removed old parquet table')
    station_df.to_parquet(station_path)
    print('   Saved parquet table')        
    
#----------------------------------------------------
#    DAILY DATA
#----------------------------------------------------

print('\n\n*** DAILY DATA *****\n\n')
for station_triplet in list(stations['triplet']):
    station = station_triplet.split(':')[0]
    print(str(station))
    
    #-- make API call ---
    params = {
        "stationTriplets": station_triplet,
        "elements": ','.join(daily_element_codes),
        "duration": 'DAILY',
        "beginDate": '1950-01-01',
        "endDate": '2099-01-01',
        "periodRef": "END",
        "centralTendencyType": "NONE",
        "returnFlags": "false",
        "returnOriginalValues": "false",
        "returnSuspectData": "false"
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        print('   API call successful')
    else:
        raise ValueError(f"   Daily data request failed with status code {response.status_code}")
    
    
    station_df = pd.DataFrame()
    station_data = data[0]['data']
    for element_item in station_data:
        element_code = element_item['stationElement']['elementCode']
        print(f'   {element_code}')
        values = element_item['values']
        df = pd.DataFrame(values).set_index('date')
        df.columns = [element_code]
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        station_df = pd.concat([station_df,df], axis=1)
    station_df = station_df.sort_index()
    station_df = station_df.dropna(how = 'all')
    station_df = station_df.interpolate(method = 'linear')

    if not os.path.exists(os.path.join(base_dir, f'daily_data/{station}')): 
        os.makedirs(os.path.join(base_dir, f'daily_data/{station}'))
        print(f'   Created /daily_data/{station} path')
    
    for dt in station_df.index:
        dt_string = dt.strftime('%Y-%m-%d')
        station_df.loc[[dt]].to_parquet(os.path.join(base_dir, f'daily_data/{station}/{dt_string}.parquet'))
    print('   parquet tables saved')
        
print('\n\n\n\n *** DONE ***')