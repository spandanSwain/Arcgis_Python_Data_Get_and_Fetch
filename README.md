# Arcgis_Python_Data_Get_and_Fetch
Python script for pushing and getting data for an Arcgis point layer. Data can be stored as CSV file or store directly in SQL server database. You can also fetch data from a CSV or Database and push it directly to point layer.

In the comments of the code I tried to explain as much as I can. The code can be further optimised.
To push the data from CSV or Database I push them as batches. As, pushing them individually takes a lot of time (in my case I had to push 24675 data from Excel and without the batch push it took 4-5 hours, after making batches it takes arounf 2-3 minutes to push them into the layer.

The columns I used in my code is for my data, in case of your data please change them to suit your column logic also those columns have to exist in your Arcgis point layer too. As, my code simply fetches and gets data from the layer, it doesn't create or check the existence of any column.
