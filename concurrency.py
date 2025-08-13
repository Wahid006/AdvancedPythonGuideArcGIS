import concurrent
import arcgis
import time

# set up ArcGIS Online credentials
username = "asfd;j"
password = "as;khd"
gis = GIS("https://www.arcgis.com", username, password)


# (2) Get the USA Structures layer
item_id_structures = "0ec8512ad21e44bb987d7e848d14e7e4"
item_structures = gis.content.get(item_id_structures)
lyr_structures = item_structures.layers[0]

# (3) Get the 2020 Wildfires layer
item_id_wildfires = "37ab7a4a05ff485aba40a53deaa20ca1"
item_wildfires = gis.content.get(item_id_wildfires)
lyr_wildfires = item_wildfires.layers[1]

# (4) Query a single wildfire feature (example: FIRE_NAME = 'AVILA')
fset_single_wildfire = lyr_wildfires.query(where="FIRE_NAME = 'AVILA'", out_fields="*")
fset_single_wildfire  # FeatureSet result


fire = fset_single_wildfire.features[0]
fire.attributes, fire.geometry


def query_structures_by_wildfire(wildfire_feature, structures_layer):
    try:
        # Get the wildfire geometry and name
        wildfire_geom = wildfire_feature.geometry
        wildfire_name = wildfire_feature.attributes["FIRE_NAME"]

        # Create a spatial filter to find structures that intersect the wildfire
        wildfire_filter = arcgis.geometry.filters.intersects(
            wildfire_geom, sr=wildfire_geom["spatialReference"]
        )

        # Query the structures layer for structures that intersect the wildfire
        structures = structures_layer.query(
            geometry_filter=wildfire_filter, return_count_only=True
        )

        # Return the wildfire name and the number of structures
        return {"Wildfire": wildfire_name, "Structures": structures}

    # If an error occurs, return the wildfire name and None for the structures
    except Exception as e:
        # Print the error so we know which wildfire failed
        print(wildfire_name, e)

        return {"Wildfire": wildfire_name, "Structures": None}


def query_structures_by_wildfire(wildfire_feature, structures_layer):
    try:
        # Get the wildfire geometry and name
        wildfire_geom = wildfire_feature.geometry
        wildfire_name = wildfire_feature.attributes["FIRE_NAME"]

        # Create a spatial filter to find structures that intersect the wildfire
        wildfire_filter = arcgis.geometry.filters.intersects(
            wildfire_geom, sr=wildfire_geom["spatialReference"]
        )

        # Query the structures layer for structures that intersect the wildfire
        structures = structures_layer.query(
            geometry_filter=wildfire_filter, return_count_only=True
        )

        # Return the wildfire name and the number of structures
        return {"Wildfire": wildfire_name, "Structures": structures}

    # If an error occurs, return the wildfire name and None for the structures
    except Exception as e:
        # Print the error so we know which wildfire failed
        print(wildfire_name, e)

        return {"Wildfire": wildfire_name, "Structures": None}


# Query a single wildfire by name
fset_single_wildfire = lyr_wildfires.query("FIRE_NAME = 'OAK' ")

# Run the function to get structure count for this wildfire
query_structures_by_wildfire(
    wildfire_feature=fset_single_wildfire.features[0], structures_layer=lyr_structures
)


# Query all the wildfires
fset_wildfires = lyr_wildfires.query(where="1=1")

import time

all_results = []

# Start a timer for the total time
total_start = time.time()

# Iterate through the wildfires
for wildfire in fset_wildfires.features:

    # Timer for individual features
    loop_start = time.perf_counter()

    # Run the query for each wildfire
    results = query_structures_by_wildfire(
        wildfire_feature=wildfire, structures_layer=lyr_structures
    )

    all_results.append(results)

    # Close out the timer
    loop_end = time.perf_counter()

    print(results, loop_end - loop_start)

# Close out the timer for total time
total_end = time.time()
print(total_end - total_start)

import time
import concurrent.futures

# Start a timer to time the whole operation
mt_start = time.time()

# Create a list to collect all the results
all_results = []

# Use a ThreadPoolExecutor to query structures for each wildfire
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

    # Create a list to store the future objects
    exec_futures = []

    # Iterate through each wildfire feature
    for wildfire in fset_wildfires.features:

        # Submit a query task for each wildfire
        exec_result = executor.submit(
            query_structures_by_wildfire,  # our function
            wildfire_feature=wildfire,  # parameters for our function
            structures_layer=lyr_structures,
        )

        # Append the future object to the list
        exec_futures.append(exec_result)

    # Iterate through the future objects as they complete
    for f in concurrent.futures.as_completed(exec_futures):
        all_results.append(f.result())

# End timer and print the total time
mt_end = time.time()
print(mt_end - mt_start)
