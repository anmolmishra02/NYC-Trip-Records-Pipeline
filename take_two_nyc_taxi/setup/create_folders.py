
# Create source data folders in the Unity Catalog Volume

catalog = "workspace"
schema = "nyc_taxi"
volume = "source_data"

base_path = f"/Volumes/{catalog}/{schema}/{volume}"

dbutils.fs.mkdirs(f"{base_path}/Yellow")
dbutils.fs.mkdirs(f"{base_path}/green")

print("Source folders created successfully:")
print(f"{base_path}/Yellow")
print(f"{base_path}/green")
