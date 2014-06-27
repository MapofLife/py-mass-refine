py-mass-refine
=========

python cave-man script for range to park intersections

To run the script, create a file called .ee-properties from the .ee-properties template

    [Authentication]
    ServiceAccount: <your service account>
    PrivateKeyFile: <path to your p.12 file>

    [Cartodb]
    APIKey: <cartodb api key>
    
You can choose to write to a csv file or to cartodb.

CSV files of species ranges are in the data directory for birds (ee_assets_iucn_birds.csv) and for amphibians, mammals and reptiles (ee_assets_iucn_amr.csv).

The script takes two parameters: the name of the species file, and the name of the ouput table or csv.  For example, to run the birds list and output to a file called myoutput.csv, run the following:

    python src/range_to_park.py ee_assets_iucn_birds.csv myoutput
    
If you want to target a single species, uncomment the following line and enter your target name

    if scientificname != 'Leptotila rufaxilla': continue

  

