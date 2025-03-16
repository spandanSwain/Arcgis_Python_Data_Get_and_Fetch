# Part 1: Push Data to Layers from Excel or Database. Refer Poles_0.csv as main input source.
# Part 2: Get Data from layers and store it in Excel or Database.
# If user wants to insert the features into layer from Excel change the 'config["entry"]["from"]' to excel, else if user wants to push data from database change 'config["entry"]["from"]' to database in the "config.ini"
from arcgis.gis import GIS
import pandas as pd
from pyproj import Transformer
import pyodbc
import configparser
import re
from datetime import datetime

# Functions
def pushing_data_to_layer():
    # This function is active when ["mode"]["mode"] is push
    print("Pushing Data to Layer")
    # Functions for pushing_data_to_layer()

    def get_data_from_excel(layer):
        csv_file = r"your_csv_file_location.csv"
        df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
        main_df = df[['POLE TYPE', 'Created By', 'COMPANY', 'ROW', 'VEG TYPE', 'VEG SIDE',
                      'LAND TYPE', 'Debris Removal', 'USER NAME', 'VERIFIED BY', 'x', 'y']]
        main_df.loc[:, 'ROW'] = main_df['ROW'].fillna(0).astype(int)
        features_df = dataframe_for_data_in_excel(main_df)
        batch_size = 10000
        upload_in_batches(features_df, batch_size, layer)

    def get_data_from_database(layer):
        # Using stored procedure in SQL server (ShowLayerData is the name of the stored procedure)
        cur.execute('EXEC ShowLayerData')
        rows = cur.fetchall()
        if not rows:
            print("No Data found in Table please check the database")
            return
        else:
            features_df = dataframe_for_data_in_database(rows)
        batch_size = 10000
        upload_in_batches(features_df, batch_size, layer)

    def dataframe_for_data_in_excel(df):
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        features_df = df.apply(lambda row: {
            # if x and y are in 4362 format then use transformer.transform else directly pass the 3857 values
            "geometry": {
                "x": transformer.transform(float(row['x']), float(row['y']))[0] if check_geometry(
                    float(row['x'])) else float(row['x']),
                "y": transformer.transform(float(row['x']), float(row['y']))[1] if check_geometry(
                    float(row['y'])) else float(row['y']),
                "spatialReference": {'wkid': 3857}
            },
            "attributes": {
                "POLE_TYPE": str(row['POLE TYPE']),
                "Created_By": str(row['Created By']),
                "COMPANY": str(row['COMPANY']),
                "ROW": int(row['ROW']),
                "VEG_TYPE": str(row['VEG TYPE']),
                "VEG_SIDE": str(row['VEG SIDE']),
                "LAND_TYPE": str(row['LAND TYPE']),
                "Debris_Removal": str(row['Debris Removal']),
                "USER_NAME": str(row['USER NAME']),
                "VERIFIED_BY": str(row['VERIFIED BY']),
                "x": float(row['x']),
                "y": float(row['y'])
            }
            # For attributes push the data in 4362 format, but for geometry push the data in 3857 format
        }, axis=1).tolist()
        return features_df

    def dataframe_for_data_in_database(rows):
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        features_df = []
        for row in rows:
            attributes = {
                "POLE_TYPE": str(row[1]),
                "Created_By": str(row[2]),
                "COMPANY": str(row[3]),
                "ROW": int(row[4]),
                "VEG_TYPE": str(row[5]),
                "VEG_SIDE": str(row[6]),
                "LAND_TYPE": str(row[7]),
                "Debris_Removal": str(row[8]),
                "USER_NAME": str(row[9]),
                "VERIFIED_BY": str(row[10]),
                "x": float(row[11]),
                "y": float(row[12])
            }
            features = {
                # if x and y are in 4362 format then use transformer.transform else directly pass the 3857 values
                "geometry": {
                    "x": transformer.transform(float(row[11]), float(row[12]))[0] if check_geometry(
                    float(row[11])) else float(row[12]),
                    "y": transformer.transform(float(row[11]), float(row[12]))[1] if check_geometry(
                    float(row[11])) else float(row[12]),
                    "spatialReference": {'wkid': 3857}
                },
                "attributes": attributes
            }
            # For attributes push the data in 4362 format, but for geometry push the data in 3857 format
            features_df.append(features)
        return features_df

    def upload_in_batches(features_df, batch_size, layer):
        print(f"Total records: {len(features_df)}")
        for i in range(0, len(features_df), batch_size):
            batch = features_df[i:i + batch_size]
            response = layer.edit_features(adds=batch)
            if response["addResults"][0]["success"]:
                print(f"Added {len(response['addResults'])}")
            else:
                # (i//batch_size)+1 => 200//100 = 2
                print(f"Error adding batch {i // batch_size + 1} {response}")
        print("Data upload completed.")

    # Outside all the functions of pushing_data_to_layer
    # Choose one layer_id for execution
    # feature_layer_item_id = config['arcgis']['layer_id_main']
    # get the layer id from point layer feature service link or open the point layer in arcgis online website, check the url and copy the id for layer id
    feature_layer_item_id = config['arcgis']['layer_id_temp']
    feature_layer = gis.content.get(feature_layer_item_id).layers[0]

    # Instead of config.ini you can use your own logic
    if config["entry"]["fromLayer"] == "excel":
        print("Pushing Data to Layer from Excel")
        get_data_from_excel(feature_layer)
    elif config["entry"]["fromLayer"] == "database":
        print("Pushing Data to Layer from Database")
        get_data_from_database(feature_layer)
    else:
        print("Error in config.ini")
# END OF pushing_data_to_layer

def getting_data_from_layer():
    # This function is active when ["mode"]["mode"] is get
    print("Getting Data from Layer")
    # Functions for getting_data_from_layer()

    def store_data_in_excel(features):
        df = data_format(features)
        current_time = datetime.now()
        try:
            unique_id = current_time.strftime("%Y%m%d_%H%M%S")
            df.to_csv(f"xylem_csv_{unique_id}.csv")
            print("Converted to CSV File")
        except Exception as e:
            print(f"File with the same name already exists:\n {e}")

    def store_data_in_database(features):
        df = data_format(features)
        push_to_database(df)

    def data_format(features):
        data = []
        for feature in features:
            attributes = feature.attributes
            geometry = feature.geometry
            features_dictionary = {
                    "attributes": attributes,
                    "geometry": geometry
            }
            data.append(features_dictionary)

        df = pd.DataFrame(data)
        attributes_dataframe = df["attributes"].apply(pd.Series)
        attributes_dataframe["x"] = attributes_dataframe["x"].astype(float)
        attributes_dataframe["y"] = attributes_dataframe["y"].astype(float)
        df = pd.concat([df, attributes_dataframe], axis=1)
        df.drop(columns=["attributes", "geometry"], inplace=True)
        main_df = df[
            ['POLE_TYPE', 'Created_By', 'COMPANY', 'ROW', 'VEG_TYPE', 'VEG_SIDE', 'LAND_TYPE', 'Debris_Removal',
             'USER_NAME', 'VERIFIED_BY', 'x', 'y']]
        main_df = main_df.copy()
        main_df.rename(columns={
            'POLE_TYPE': 'Pole Type',
            'Created_By': 'Created By',
            'COMPANY': 'COMPANY',
            'ROW': 'ROW',
            'VEG_TYPE': 'VEG TYPE',
            'VEG_SIDE': 'VEG_SIDE',
            'LAND_TYPE': 'LAND_TYPE',
            'Debris_Removal': 'Debris Removal',
            'USER_NAME': 'USER NAME',
            'VERIFIED_BY': 'VERIFIED BY',
            'x': 'x',
            'y': 'y'
        }, inplace=True)
        return main_df

    def push_to_database(df):
        item_number = 0
        for _, row in df.iterrows():
            try:
                values = (row["Pole Type"], row["Created By"], row["COMPANY"], row["ROW"],
                          row["VEG TYPE"], row["VEG_SIDE"], row["LAND_TYPE"], row["Debris Removal"],
                          row["USER NAME"], row["VERIFIED BY"], row["x"], row["y"])

                # For Stored Procedure: 'EXEC stored_procedure_name ?,?,...' (? = Parameters)
                cur.execute("EXEC FromLayerTODB ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", values)
                item_number += 1
                print(f"Pushed item {item_number} successfully")
                con.commit()
            except Exception as e:
                print(f"Error in pushing the data. Root cause: {e}")

    # Outside all the functions of getting_data_from_layer
    # Choose one layer_id for execution
    # feature_layer_item_id = config['arcgis']['layer_id_main']
    feature_layer_item_id = config['arcgis']['layer_id_temp']
    feature_layer = gis.content.get(feature_layer_item_id).layers[0]
    queried_features = feature_layer.query().features

    if config["entry"]["toLayer"] == "excel":
        print("Pushing Data to Excel")
        store_data_in_excel(queried_features)
    elif config["entry"]["toLayer"] == "database":
        print("Pushing Data to Database")
        store_data_in_database(queried_features)
        cur.close()
        con.close()
    else:
        print("Error in config.ini")
# END OF getting_data_from_layer

def check_geometry(point):
    pattern = r"^-?\d+\.\d+$"
    if re.match(pattern, str(point)):
        # if data is in 4362 format then it returns True
        return True
    else:
        # if data is in 3857 format then it returns False
        return False
# END OF check_geometry


# MAIN FUNCTION
# SETTING UP CONFIG
config = configparser.ConfigParser()
config.read('config.ini')
# GIS Login
gis = GIS(config["arcgis"]["url"], config["arcgis"]["username"], config["arcgis"]["password"])
print("Logged in")
# Database Connection
con = pyodbc.connect(
    "DRIVER={SQL Server};"
    f"SERVER={config['database']['server']};"
    f"DATABASE={config['database']['database']};"
    "Trusted_Connection=yes;"
)
cur = con.cursor()
print("Database Connection established")

# push or get mode
if config["mode"]["mode"] == "push":
    pushing_data_to_layer()
elif config["mode"]["mode"] == "get":
    getting_data_from_layer()
else:
    print("Error in config.ini")