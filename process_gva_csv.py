import pandas as pd
from sys import argv
from geopy.geocoders import Nominatim  # free geodcoding service
import geopy.exc

# Original headers to new headers mapping
HEADER_MAP = {
    "Incident ID": "gva_id",
    "Incident Date": "start_date",
    "State": "state",
    "City Or County": "city",
    "Address": "address",
    "# Killed": "deaths",
    "# Injured": "wounded",
}

# State names to abbreviations mapping
STATE_ABBREVIATIONS = {
  "Alabama": "AL",
  "Alaska": "AK",
  "Arizona": "AZ",
  "Arkansas": "AR",
  "California": "CA",
  "Colorado": "CO",
  "Connecticut": "CT",
  "Delaware": "DE",
  "District Of Columbia": "DC",
  "Florida": "FL",
  "Georgia": "GA",
  "Hawaii": "HI",
  "Idaho": "ID",
  "Illinois": "IL",
  "Indiana": "IN",
  "Iowa": "IA",
  "Kansas": "KS",
  "Kentucky": "KY",
  "Louisiana": "LA",
  "Maine": "ME",
  "Maryland": "MD",
  "Massachusetts": "MA",
  "Michigan": "MI",
  "Minnesota": "MN",
  "Mississippi": "MS",
  "Missouri": "MO",
  "Montana": "MT",
  "Nebraska": "NE",
  "Nevada": "NV",
  "New Hampshire": "NH",
  "New Jersey": "NJ",
  "New Mexico": "NM",
  "New York": "NY",
  "North Carolina": "NC",
  "North Dakota": "ND",
  "Ohio": "OH",
  "Oklahoma": "OK",
  "Oregon": "OR",
  "Pennsylvania": "PA",
  "Rhode Island": "RI",
  "South Carolina": "SC",
  "South Dakota": "SD",
  "Tennessee": "TN",
  "Texas": "TX",
  "Utah": "UT",
  "Vermont": "VT",
  "Virginia": "VA",
  "Washington": "WA",
  "West Virginia": "WV",
  "Wisconsin": "WI",
  "Wyoming": "WY"
}

geolocator = Nominatim(user_agent="MassShootingDatabase")


# This function must come after the declaration of geolocator
def geocode(address:str="", city:str="", state:str="") -> list[geopy.location.Location]:
    '''Returns None, geopy.location.Location or list of them if exactly_one=False.

        Raises GeocoderTimedOut
    '''
    query = {"address": address, "city": city, "state": state}
    return geolocator.geocode(query, addressdetails=True, geometry="geojson")


# program begins here!
SOURCE_CSV = ""
TARGET_CSV = ""

if len(argv) >= 3:
    SOURCE_CSV = argv[1]  # Change this later to an command line arg
    TARGET_CSV = argv[2]  # ditto
elif argv[1] == "help":
    print("process_gva_csv.py <csv_to_process> <csv_to_output_to>")
    print("Windows: python process_gva_csv.py target.csv out.csv")
    print("Linux: python3 process_gva_csv.py target.csv out.csv")
    exit()
else:
    print("Not enough arguments!")
    exit()

try:
    csv_data = pd.read_csv(SOURCE_CSV)  # make this a data frame
except FileNotFoundError:
    print("Input file doesn't exist!")
    exit()


try:
    csv_data.pop("Operations")          # this column is useless
except KeyError:
    print("Not a CSV file!")
    exit()

if csv_data.columns != HEADER_MAP.keys():
    print("Not a CSV from the Gun Violence Archive!")
    exit()

csv_data.rename(columns=HEADER_MAP, inplace=True)   # rename

# add columns and reposition existing ones
# Format dataframe to the new Incidents spreadsheet
blanks = ["" for i in range(csv_data.shape[0])]
csv_data.insert(1, "incident_name", blanks)
csv_data.insert(2, "place_type", blanks)
csv_data.insert(4, "end_date", blanks)

# by popping most of the inorder columns out first...
# we can just add new columns at the end
city_col = csv_data.pop("city")
state_col = csv_data.pop("state")
deaths_col = csv_data.pop("deaths")
wounded_col = csv_data.pop("wounded")

csv_data["city"] = city_col
csv_data["state"] = state_col
csv_data["postal_code"] = blanks
csv_data["congressional"] = blanks
csv_data["state_house"] = blanks
csv_data["lat"] = blanks
csv_data["long"] = blanks
csv_data["deaths"] = deaths_col
csv_data["wounded"] = wounded_col
csv_data["postal_code"] = blanks
csv_data["transferred"] = blanks
csv_data["fact_check1"] = blanks
csv_data["fact_check2"] = blanks
csv_data["fact_check3"] = blanks

# convert the existing date column to ISO-8601 time stamps
# isoformat method is too precise and doesn't add Z at the end
csv_data["start_date"] = csv_data["start_date"].apply(
    func=lambda x: pd.Timestamp(x).strftime("%Y-%m-%dT%H:%SZ"))

# geocode address / city
# these arrays are used to fill in some columns...
cities = []
states = []
postal_codes = []
lats = []
longs = []
rows = 1

for row in csv_data.itertuples():
    city = ""
    state = ""
    postal_code = ""
    lat = ""
    long = ""

    try:
        location = geocode(row.address, row.city, row.state)
    except geopy.exc.GeocoderTimedOut as e:
        print(e)
        print("TIMED OUT | ", end="")
    except geopy.exc.GeocoderQuotaExceeded as e:
        print(e)
        print("QUOTA EXCEEDED | ", end="")
    except geopy.exc.GeocoderRateLimited as e:
        print(e)
        print("RATE LIMITED | ", end="")
    except geopy.exc.GeocoderUnavailable as e:
        print(e)
        print("UNAVAILABLE | ", end="")
    except geopy.exc.GeocoderServiceError as e:
        print(e)
        print("SERVICE ERROR | {} | ".format(row.address), end="")
    else:
        if location is not None:
            if 'city' in location.raw["address"]:
                city = (location.raw["address"]["city"])
            if 'state' in location.raw["address"]:
                state =\
                    STATE_ABBREVIATIONS[location.raw["address"]["state"]]
            if 'postal_code' in location.raw["address"]:
                postal_codes =\
                    STATE_ABBREVIATIONS[location.raw["address"]["postal_code"]]
            lat = location.latitude
            long = location.longitude
    finally:
        cities.append(city)
        states.append(state)
        postal_codes.append(postal_code)
        lats.append(lat)
        longs.append(long)

        progress_log =\
            "Row {:06}/{:06}| ".format(rows, csv_data.shape[0]) +\
            "City: {:20} State: {:2} ".format(city, state) +\
            "ZIP: {:5} Coords: {:5.5}, {:5.5}".format(postal_code, lat, long)
        print(progress_log)
        rows += 1

# prepare dataframe to be exported... fill out remanining columns
csv_data["city"] = cities
csv_data["state"] = states
csv_data["postal_code"] = postal_codes
csv_data["lat"] = lats
csv_data["long"] = longs

# write to CSV
while True:
    try:
        csv_data.to_csv(TARGET_CSV, index=False)
    except PermissionError:
        print("Please close whatever program has this file open.")
    else:
        print("Successfully written to file!")
        break
