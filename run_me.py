import ftplib
import os
import shutil
from datetime import date
from datetime import datetime
from datetime import timedelta

import pandas as pd
import xarray as xr
from geopy.geocoders import Nominatim
from meteostat import Point, Daily

# would not recommend, bad practice
location = None

# details to connect to the ESA server
connection_url = "smos-diss.eo.esa.int"
user_name = "provide user name here"
password = "provide your password here"
# You can also ask us (it does actually work)

# the file path where all the files are stored that we want
url_path_before = "/SMOS/L2SM/MIR_SMUDP2_nc"

# file path where we store them temporarily
file_path_storage = "downloads/"

# this function reads our list of plants and water requirements
# and returns all those that need to be watered
def import_plants(water_liters):
    df = pd.read_csv('data/water_usage.csv', sep=';')
                        #dtype={'Täglicher Wasserbedarf Min': float,
                        #       'Täglicher Wasserbedarf Max':float})
    df = df.rename(columns={"Täglicher Wasserbedarf Min": "min", "Täglicher Wasserbedarf Max": "max"})
    return df.query('min > @water_liters')

def connect_to_server(date_given):
    # Connect to the server and login
    ftp = ftplib.FTP(connection_url)
    ftp.login(user_name, password)

    # build the full path to the file we want, using the previously specified date
    query_full_path = url_path_before + "/" + str(date_given.year) + "/" + date_given.strftime('%m') + "/" + date_given.strftime('%d')
    # navigate to the file path in the server
    ftp.cwd(query_full_path)

    # get filenames within the directory
    filenames = ftp.nlst()

    # make a directory for data, if it doesnt exist
    try:
        # Create target Directory
        os.makedirs(file_path_storage)
    except FileExistsError:
        print("Directory already exists.")

    # go through the list of all the files there and download all
    for filename in filenames:
        with open(file_path_storage + filename, "wb") as file:
            # use FTP's RETR command to download the file
            ftp.retrbinary(f"RETR {filename}", file.write)

    # stolen from: https://stackoverflow.com/questions/5230966/
    # python-ftp-download-all-files-in-directory

    # disconnect from the server
    ftp.quit()


def comb_for_soil(target_long, target_lat):
    # get a list of all the filenames in specified directory
    all_files = [x for x in os.listdir(file_path_storage) if x.endswith('.nc')]
    # create an empty dictionary for storing them
    datasets_dict = {}
    # open every .nc file, convert to a dataframe and then store it
    for filename in all_files:
        opened_dataset = xr.open_dataset(file_path_storage + filename)
        datasets_dict[filename] = opened_dataset.to_dataframe()

    # let's create a giant dataframe of everything
    big_dataframe = pd.concat(datasets_dict.values(), ignore_index=True)

    # but we only need 3 columns
    big_dataframe_clean = big_dataframe[['Soil_Moisture', 'Longitude', 'Latitude']]

    # and we only need those rows that actually have data
    big_dataframe_clean = big_dataframe_clean[big_dataframe_clean.Soil_Moisture.notnull()]

    # slightly annnoyingly verbose calculation, but it works
    # See which distance is closest in longitude/latitude
    # add the distances to find the closest possible distance
    big_dataframe_clean["distance_longitude"] = abs(big_dataframe_clean["Longitude"] \
                                                    - target_long)
    big_dataframe_clean["distance_latitude"] = abs(big_dataframe_clean["Latitude"] \
                                                   - target_lat)
    big_dataframe_clean["combined_distances"] = big_dataframe_clean["distance_longitude"] \
                                                + big_dataframe_clean["distance_latitude"]

    # Sort the whole dataframe to find the closest possible match for the specified
    # target, and only extract the relevant soil moisture measurement
    # if anyone is interested, we can also tell them how close that measurement
    # actually is
    soil_moisture = big_dataframe_clean.sort_values(["combined_distances"])[0:1]["Soil_Moisture"].values[0]
    long_found = big_dataframe_clean.sort_values(["combined_distances"])[0:1]["Longitude"].values[0]
    lat_found = big_dataframe_clean.sort_values(["combined_distances"])[0:1]["Latitude"].values[0]
    try:
        shutil.rmtree(file_path_storage)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

    return soil_moisture

def get_adress():
    address = input("\nWo befinden Sie sich?")
    geolocator = Nominatim(user_agent="TechLabs_MS_ProjectPlants")
    # again, not recommended
    global location
    location = geolocator.geocode(address)
    if location is None:
        return False
    else:
        return True


# this runs while there is not yet a location found for the project
while (not get_adress()):
    print("\nTut mir Leid, den Ort kenne ich nicht.")

# Show the user the location that was found
print("\nIhre Berechnung basiert auf dem folgenden Ort: \n" + location.address)

# Set time period
today = date.today()
# used for testing when we looked at past days
yday = today - timedelta(days=0)
# the time must be set at 00:00, otherwise empty results are returned
today_datetime = datetime(yday.year, yday.month, yday.day)

# Create Point for user specified location
location_point = Point(location.latitude, location.longitude)

# Get daily data for today
data = Daily(location_point, today_datetime, today_datetime)
data = data.fetch()
# print(data)

# get soil data
time_available = input("\nHaben Sie etwas Zeit? Dann schaue ich auch nach der Bodenfeuchtigkeit: [j/n]")
if time_available == "j":
    print("\nOkay, ich lade die Daten.")
    connect_to_server(today_datetime)
    soil_moisture = comb_for_soil(location.longitude, location.latitude)
else:
    soil_moisture = 0

# check if the sunshine length has been recorded for today
if data.empty:
    print("\nTut mir Leid, ich habe leider keine Daten für diesen Ort oder dieses Datum gefunden.")
elif pd.isna(data.iloc[0]["tsun"]):
    print("\nIch bin mir leider nicht ganz sicher, ob Sie heute gießen sollten.")
else:
    # calculate the available water for all plants
    water_available = 0, 8 * soil_moisture + data.iloc[0]["prcp"] - (data.iloc[0]["tsun"] * 0.016)

    # get a list of all the plants
    to_be_watered_plants = import_plants(water_available)
    # if there is enough water for all, we are good
    if to_be_watered_plants.empty:
        print("\nEs muss nichts gegossen werden.")
    else:
        # start with an empty dataframe and ask the user for details on what plants they have
        plants_watered_here = pd.DataFrame()
        for plant_type in to_be_watered_plants["Pflanzeart"].unique():
            answer = input("Haben Sie " + plant_type + "? [j/n]")
            if answer == "j":
                plants_watered_here = plants_watered_here.append(
                    to_be_watered_plants.query("Pflanzeart == @plant_type"))
        if not plants_watered_here.empty:
            print("\nWenn Sie die folgenden Pflanzen haben, sollten Sie sie gießen: ")
            print(plants_watered_here["Pflanze"].to_string(index=False))
        else:
            print("\nWer nichts hat, braucht auch nichts zu gießen.")