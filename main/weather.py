import requests
import pandas as pd
from airflow.hooks.base import BaseHook
import io


APIKey = 'Open weather map Key'
BaseURL = 'http://api.openweathermap.org/data/2.5/weather'

def get_weather():
    df = pd.DataFrame(columns=['Location','Temprature','feels_like', 'humidity','visibility','Wind_speed'])
    global weather_report
    list1 = []

    City_names = ['Toronto','Vancouver','Montreal','Ottawa','Victoria','Los Angeles','Miami','Sydney','Houston',
                  'Washington','London','Hong Kong','Singapore','Tokyo']
    for city in City_names:
        RequestURL = f'{BaseURL}?appid={APIKey}&q={city}'
        Response = requests.get (RequestURL)
        if Response.status_code == 200:
            Data = Response.json ()
            print(Data)
        else:
            print("'Something went wrong'")
        refined = {'Location' : city,
                   'Temprature' : round(Data['main']['temp']-273.15),
                   'feels_like' : round(Data['main']['feels_like']-273.15),
                   'humidity' : Data['main']['humidity'],
                   'visibility' : Data['visibility'],
                   'Wind_speed' : Data['wind']['speed'],
                   'pressure' : Data['main']['pressure']}

        list1.append(refined)
        df = pd.DataFrame.from_dict(list1)
        weather_report = df
    # print(df)


def upload_csv_to_gcs():
    # Create or load your DataFrame
    csv_string = weather_report.to_csv(index=False)

    # Define GCS bucket and object path
    bucket_name = 'your-gcs-bucket-name'
    object_path = 'path/in/gcs/folder/file.csv'

    # Get Google Cloud Storage connection
    gcs_conn_id = 'google_cloud_default'  # The connection ID for GCS in Airflow
    gcs_hook = BaseHook.get_connection(gcs_conn_id)
    gcs_bucket = gcs_hook.extra_dejson.get('bucket')

    # Upload CSV string to GCS
    with io.BytesIO(csv_string.encode()) as f:
        hook = gcs_hook.get_conn()
        hook.upload(bucket_name, object_path, f)

