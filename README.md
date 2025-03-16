# ArcGIS Python Data Get & Fetch

This Python script is designed for **pushing and retrieving data** for an ARCGIS point layer. Data can be stored as a **CSV file** or directly in an **SQL Server database**. You can also fetch data from these sources and push it directly to the ArcGIS point layer.

Pushing data **individually** is slow!  
For example, inserting **24,675 records** from an Excel file **took 4-5 hours**.  
After implementing **batch processing**, the same operation now takes **only 2-3 minutes**.  

- The script **fetches and pushes data** but **does not** create or check the existence of columns.  
- Modify column names to match **your dataset** and ensure they exist in your **ARCGIS point layer**.  
- **Sensitive information** (ArcGIS username, password, URL, feature layer ID) is stored in `config.ini`.  

---

### For `config.ini`

Create a `config.ini` file in the project directory to store your ARCGIS credentials: (or you can ignore the `config.ini` and simply type the username and layer IDs.)

```ini
[arcgis]
USERNAME = your_username
PASSWORD = your_password
URL = your_company_url
FEATURE_LAYER_ID = your_feature_layer_id
```
