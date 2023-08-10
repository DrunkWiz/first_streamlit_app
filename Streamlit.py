import streamlit as st
import pandas as pd
import joblib
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark.window import Window
from sklearn import preprocessing # https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/sp4py_utilities
from snowflake.snowpark.functions import col

import getpass
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import math
from datetime import timedelta


import folium
from streamlit_folium import st_folium
import openrouteservice as ors
import operator
from functools import reduce

st.set_page_config(layout="wide")

X_final_scaled=pd.read_csv('x_final_scaled.csv')
truck_location_df=pd.read_csv('truck_manager_merged_df.csv')

truck_location_df["location_visited"] = truck_location_df["location_visited"].apply(eval)
truck_location_df["predicted_earning"] = truck_location_df["predicted_earning"].apply(eval)
truck_df_exploded = truck_location_df.explode(["location_visited", "predicted_earning"], ignore_index=True)
truck_df_exploded["shift"] = truck_df_exploded.groupby("Truck_ID").cumcount() + 1
all_locations = truck_df_exploded["location_visited"].tolist()

# Find the latitude and longitude for each location
location_lat_long = {}
for location in all_locations:
    location_info = X_final_scaled[X_final_scaled["LOCATION_ID"] == location]
    if not location_info.empty:
        lat = location_info["LAT"].values[0]
        long = location_info["LONG"].values[0]
        location_lat_long[location] = (lat, long)

def get_lat_long(location_list):
    lat_list = []
    long_list = []
    for loc in location_list:
        if loc in location_lat_long:
            lat, long = location_lat_long[loc]
            lat_list.append(lat)
            long_list.append(long)
        else:
            lat_list.append(None)
            long_list.append(None)
    return lat_list, long_list

lat, lon = get_lat_long(truck_df_exploded["location_visited"])

truck_df_exploded["Lat"] = lat
truck_df_exploded["Lon"] = lon


#ors client
ors_client = ors.Client(key='5b3ce3597851110001cf6248d282bc5c5d534216a412fa4e36a497e3')

# Define a function to get the route between two points using ORS
def get_route(start_point, end_point):
        radius = 500  # 10 kilometers
        profile = 'driving-car'
        try:
                # Get the route between the start and end points
                route = ors_client.directions(
                coordinates=[start_point, end_point],
                profile=profile,
                format='geojson',
                radiuses=[radius, radius]
                )
                return route

        except ors.exceptions.ApiError as e:
                print(e)
                return None

def filter_truck_data(truck_id):
    return truck_df_exploded[truck_df_exploded['Truck_ID'] == truck_id]

def create_map(selected_truck_ids):
        # Check if truck IDs are selected
        if selected_truck_ids:

                # Get the data for the first selected truck
                selected_truck_data = filter_truck_data(selected_truck_ids[0])

                # FOLIUM MAP
                m = folium.Map(location=[selected_truck_data['Lat'].iloc[0], selected_truck_data['Lon'].iloc[0]], zoom_start=13)

                # Iterate through selected truck IDs to display each truck route
                for selected_truck_id in selected_truck_ids:
                        # Filter truck data based on the selected truck ID
                        selected_truck_data = filter_truck_data(selected_truck_id)

                        place_lat = selected_truck_data['Lat'].astype(float).tolist()
                        place_lng = selected_truck_data['Lon'].astype(float).tolist()

                        points = []

                        # read a series of points from coordinates and assign them to points object
                        for i in range(len(place_lat)):
                                points.append([place_lat[i], place_lng[i]])

                        # Choose a different polyline color for each truck route
                        colors = ['darkred', 'black', 'blue', 'orange', 'lightblue', 'lightgreen', 'purple', 'gray', 'white', 'cadetblue', 'darkgreen', 'pink', 'darkblue', 'lightgray', 'beige']
                        color_index = available_trucks.index(selected_truck_id)
                        polyline_color = colors[color_index % len(colors)]

                        folium.PolyLine(points, color=polyline_color, dash_array='5', opacity='0.6',
                                        tooltip=f'Truck Route {selected_truck_id}').add_to(m)


                        # Specify marker color based on polyline color
                        marker_color = 'white' if polyline_color != 'white' else 'black'

                        # Add markers for each truck location
                        for index, lat in enumerate(place_lat):
                                folium.Marker([lat, place_lng[index]],
                                        popup=('Truck Location {} \n '.format(index)),
                                        icon=folium.Icon(color=polyline_color, icon_color=marker_color, prefix='fa', icon='truck')
                                        ).add_to(m)
                        
                        for i in range(len(place_lat) - 1):
                                start_point = [place_lng[i], place_lat[i]]  # Corrected order: [long, lat]
                                end_point = [place_lng[i + 1], place_lat[i + 1]]  # Corrected order: [long, lat]

                                # Check if the start point and end point are the same
                                if start_point != end_point:
                                        # Get the route between two consecutive points
                                        route = get_route(start_point, end_point)

                                        # Check if the route is found
                                        if route is not None:

                                                # print(route_coords)
                                                waypoints = list(dict.fromkeys(reduce(operator.concat, list(map(lambda step: step['way_points'], route['features'][0]['properties']['segments'][0]['steps'])))))

                                                folium.PolyLine(locations=[list(reversed(coord)) for coord in route['features'][0]['geometry']['coordinates']], color=polyline_color).add_to(m)

                                                # folium.PolyLine(locations=[list(reversed(route['features'][0]['geometry']['coordinates'][index])) for index in waypoints], color="red").add_to(m)
                
                                        else:
                                                print(f"No route found between {start_point} and {end_point}")
                                else:
                                        print(f"Start point and end point are the same: {start_point}")

                        # Convert 'predicted_earning' to numeric (float) and round to 0 decimal places
                        truck_df_exploded['predicted_earning'] = truck_df_exploded['predicted_earning'].astype(int)

                        # Calculate total revenue for each truck with 0 decimal places
                        total_revenue_per_truck = truck_df_exploded.groupby('Truck_ID')['predicted_earning'].sum()

                        truck_info = {
                        'Truck Manager Name': [],  # Add the truck manager name as the first column
                        'City': [],
                        'Truck ID🚚': [],  # Updated column name
                        'Number of Shifts': [],
                        'Total Revenue💵': [],
                        'Truck Colour': []  # Updated column name
                        }

                # Populate the truck_info dictionary with data
                for selected_truck_id in selected_truck_ids:
                        # Get the truck manager name for the selected truck ID
                        selected_truck_manager = truck_location_df[truck_location_df['Truck_ID'] == selected_truck_id]['Name'].iloc[0]
                        truck_info['Truck Manager Name'].append(selected_truck_manager)
                        selected_truck_manager = truck_location_df[truck_location_df['Truck_ID'] == selected_truck_id]['City'].iloc[0]
                        truck_info['City'].append(selected_truck_manager)
                        truck_info['Truck ID🚚'].append(selected_truck_id)
                        selected_truck_data = filter_truck_data(selected_truck_id)
                        num_shifts = selected_truck_data['shift'].max()
                        truck_info['Number of Shifts'].append(num_shifts)
                        total_revenue = total_revenue_per_truck.get(selected_truck_id, 0)
                        truck_info['Total Revenue💵'].append(total_revenue)
                        color_index = available_trucks.index(selected_truck_id)
                        truck_color = colors[color_index % len(colors)]
                        truck_info['Truck Colour'].append(truck_color)
                        
                # Convert the truck_info dictionary to a DataFrame
                truck_info_df = pd.DataFrame(truck_info)

                # Create a new column "Colour" with background colors
                truck_info_df['Colour'] = truck_info_df['Truck Colour']

                # Display the truck information table with colored cells and black text for color names
                st.subheader('Truck Information')

                # Custom CSS style to display colored cells with white text and black text for color names
                def style_color_cells(val):
                        return f'background-color: {val}; color: {val}; text-align: center;'

                # Apply the custom style to the 'Truck Color' column
                styled_truck_info_df = truck_info_df.style.applymap(style_color_cells, subset=['Colour'])

                # Display the styled DataFrame using st.dataframe
                st.dataframe(styled_truck_info_df)

                # Display the map with the selected truck routes
        st_folium(m, width=1500)

## STREAMLIT APP

st.header('Routing Map🗺️')
st.subheader('Choose a Truck Manager')

# Extract a list of unique names from the "Name" column
unique_names = truck_location_df['Name'].unique().tolist()

# Create a multiselect widget to choose one or more names
selected_names = st.multiselect("Select Truck Manager Name", unique_names)

# Filter the available trucks based on the chosen names
available_trucks = truck_location_df[truck_location_df['Name'].isin(selected_names)]['Truck_ID'].tolist()

# Check if 'prev_selected_truck_ids' exists in session state
if 'prev_selected_truck_ids' not in st.session_state:
    st.session_state.prev_selected_truck_ids = []

# Display the truck selection section
st.subheader('Choose a truck 🚚')
selected_truck_ids = st.multiselect("Select Truck IDs 🌮🍦", available_trucks)

# Add a "Run Map" button
with st.form("RunMapForm"):
        st.form_submit_button("Run Map")

        if selected_truck_ids:
                if selected_truck_ids != st.session_state.prev_selected_truck_ids:
                        # Save the current selected truck IDs to session state
                        selected_truck_ids_str = ', '.join(str(truck_id) for truck_id in selected_truck_ids)
                        st.success(f"Your selected Truck IDs {selected_truck_ids_str} have been saved!")
                        # Create the map and display truck routes
                        create_map(selected_truck_ids)
                else:
                        st.info("Selected truck IDs have not changed. The map has not been changed.")
                        create_map(selected_truck_ids)
        else:
                st.info("No truck IDs have been selected.")

map_placeholder = st.empty()

