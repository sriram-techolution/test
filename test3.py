from hccolupdates import HCColUpdates
import pandas as pd
import numpy as np
# --- 1. Define Connection and Data Sources ---
connection_string = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=hcus-d-dea-sqlmi-01.public.b882f1675275.database.windows.net,3342;DATABASE=pos;UID=Posdataload;PWD=DGtb&7v4ghw"

# Define the queries and metadata for all lookup tables you need.
source_configurations = {
    'product_master': {
        'query': 'SELECT F001_ISBN, F001_CompanyCode, F001_Format, F001_OnsaleDate, F001_SellingCompany, F001_TAP, F001_TAR, F001_TAC, F001_TAS  FROM dbo.V001_ProductMaster_All WITH(NOLOCK)',
        'date_cols': ['F001_OnsaleDate']
    },
    'jde_master': {
        'query': "SELECT C008_ISBN, C008_CompanyCode, C008_ProgramCode, C008_Category, C008_DateOnsale, C008_SellingCompany FROM dbo.T008_JDEProductMaster WITH(NOLOCK) WHERE C008_SellingCompany IN ('30001', '80001')",
        'date_cols': ['C008_DateOnsale']
    },
    'price_history': {
        'query': 'SELECT F006_ISBN, F006_Price, F006_StartDate FROM dbo.V006_PriceHistory WITH(NOLOCK)',
        'date_cols': ['F006_StartDate'],
        'sort_by': {'by': ['F006_ISBN', 'F006_StartDate'], 'ascending': [True, False]}
    }
}

# --- 2. Initialize the Updater ---
try:
    updater = HCColUpdates(connection_string=connection_string, source_configs=source_configurations)
except Exception as e:
    print(f"Failed to initialize updater. Check connection string and queries. Error: {e}")
    exit()
