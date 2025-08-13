# ---------------------------
# Import packages
# ---------------------------
import arcpy
import os
import shutil
import pathlib
import multiprocessing

# ---------------------------
# Set up inputs
# ---------------------------

# Input file geodatabase path
input_fgdb = r".\Chapter 10.gdb"

# Input feature class name
input_fc_name = "Highways_intersect"

# Output folder
output_folder = r".\zipped_outputs"

# ---------------------------
# Create and test an input feature class path
# ---------------------------

# Combine geodatabase path and feature class name
full_fc_path = os.path.join(input_fgdb, input_fc_name)

# Test if the feature class exists
arcpy.Exists(full_fc_path)

# ---------------------------
# Generate a list of counties
# ---------------------------
counties = [r[0] for r in arcpy.da.SearchCursor(full_fc_path, ["NAMELSAD"])]
len(counties)

# Get unique counties and sort them
counties = list(set(counties))
counties.sort()
counties

# ---------------------------
# Set up an output folder
# ---------------------------
if not os.path.exists(output_folder):
    os.mkdir(output_folder)

# ---------------------------
# Setting up logic for a single county
# ---------------------------

# Pick a single county (first in the list)
county = counties[0]
county

# Replace spaces with underscores
county_no_spaces = county.replace(" ", "_")
county_no_spaces

# ---------------------------
# Create a file geodatabase
# ---------------------------
fgdb = arcpy.management.CreateFileGDB(
    out_folder_path=output_folder, out_name=f"{county_no_spaces}_Output"
)

fgdb
fgdb[0]  # Path to the created file geodatabase

# ---------------------------
# Create a feature class for the county
# ---------------------------
output_fc = arcpy.conversion.ExportFeatures(
    in_features=full_fc_path,
    out_features=os.path.join(fgdb[0], f"{county_no_spaces}_Highways"),
    where_clause=f"NAMELSAD='{county}'",
)

output_fc
output_fc[0]  # Path to the exported feature class

import pathlib
import shutil

# ---------------------------
# Compress the file geodatabase into a zip file
# ---------------------------

# Get the path for the file geodatabase you created
source_fgdb_path = pathlib.Path(fgdb[0])

# The name of the folder to place the File Geodatabase in
fgdb_folder_name = source_fgdb_path.stem

# The location of the folder to place the File Geodatabase in
fgdb_folder_location = source_fgdb_path.parent

# The path to the folder to place the File Geodatabase in
fgdb_folder_path = fgdb_folder_location.joinpath(fgdb_folder_name)

# The path of our copied File Geodatabase
fgdb_path = fgdb_folder_path.joinpath(source_fgdb_path.name)

# Copy the file geodatabase into a temporary folder (avoids locks)
shutil.copytree(source_fgdb_path, fgdb_path, ignore=shutil.ignore_patterns("*.lock"))

# Zip the File Geodatabase
zipped_fgdb = shutil.make_archive(
    base_name=fgdb_folder_path,  # name of the archive (without extension)
    format="zip",  # archive format
    root_dir=fgdb_folder_path,  # directory to archive
)

# ---------------------------
# Delete the temporary files
# ---------------------------

# Delete the geodatabase from ArcPy
arcpy.management.Delete(fgdb)

# Delete the temporary folder and copied geodatabase
shutil.rmtree(fgdb_folder_path)

# Test your function
zip_county_highways(full_fc_path, output_folder, "Butte County")

# ---------------------------
# Setting up multiprocessing
# ---------------------------

# Find out how many cores you have available
import multiprocessing

multiprocessing.cpu_count()

if __name__ == "__main__":
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # Set up the process pool executor
    with ProcessPoolExecutor(max_workers=process_count) as executor:

        # Set up a list to contain all the future objects
        futures_list = []

        # Submit each job to the executor
        for county in counties:
            futures_list.append(
                executor.submit(
                    zip_county_highways, full_fc_path, output_folder, county
                )
            )

        # Iterate through the futures to see when they're completed
        for future in as_completed(futures_list):
            print(future.result())
