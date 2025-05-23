import json
import numpy as np
import pandas as pd

SECTOR_LIST = ["CCCM", "Education", "Nutrition", "Food Security",
               "Health", "Protection", "Shelter", "WASH",
               "General Protection", "Child Protection",
               "GBV", "Mine Action", "HLP"]

REGION_MAP = {
    "AFG": "ROAP",
    "BFA": "ROWCA",
    "CAF": "ROWCA",
    "TCD": "ROWCA",
    "COL": "ROLAC",
    "COD": "ROWCA",
    "SLV": "ROLAC",
    "GTM": "ROLAC",
    "HTI": "ROLAC",
    "HND": "ROLAC",
    "MLI": "ROWCA",
    "MOZ": "ROSEA",
    "MMR": "ROAP",
    "NGA": "ROWCA",
    "SOM": "ROSEA",
    "SSD": "ROSEA",
    "SDN": "ROSEA",
    "SYR": "ROMENA",
    "UKR": "ROCCA",
    "VEN": "ROLAC",
    "YEM": "ROMENA"
}

def set_header(df):
    """
    Set row beginning with 'Admin 0' as header and drop any rows above
    """
    header_row = df[df[0].astype(str).str.startswith("Admin 0")]
    header_index = header_row.index[0]
    df = df.iloc[header_index:].reset_index(drop=True)
    df.columns = df.iloc[0]
    return df.iloc[1:].reset_index(drop=True)

def set_merge_key(df):
    """
    Set 'Admin 3 P-Code' as merge key if not empty, otherwise use 'Admin 2 P-Code'
    """
    df['merge_key'] = df.apply(
        lambda row: row['Admin 3 P-Code'] if pd.notnull(row.get('Admin 3 P-Code')) and str(
            row.get('Admin 3 P-Code')).strip() != ""
        else row.get('Admin 2 P-Code'),
        axis=1
    )
    return df

def process_data(input_file, output_file):
    """
    Process data to set header and merge sheets, matching by admin3 p-code
    if avail, otherwise admin2 p-code

    Args:
        input_file (str): path to excel file
        output_file (str): path to saved json file

    Returns:
        merged_df (DataFrame): final merged dataframe
    """
    # Process the first sheet (PiN)
    df1 = pd.read_excel(input_file, sheet_name=0, header=None)
    df1 = set_header(df1)

    # 2) Insert region_code column
    df1["Region"] = df1["ISO3"].map(REGION_MAP)
    df1["Region"] = df1["Region"].fillna("Unknown")

    # Keep columns ISO3, Population, Final PiN, and any column beginning with "Admin"
    base_cols = ["ISO3", "Region", "Population", "Final PiN"]
    cols_to_keep = [
        col
        for col in df1.columns
        if str(col).startswith("Admin")
           or col in (base_cols + SECTOR_LIST)
    ]
    df1 = df1[cols_to_keep]

    # Convert cols to numeric
    numeric_cols = ["Population", "Final PiN"] + SECTOR_LIST
    df1[numeric_cols] = df1[numeric_cols].apply(
        pd.to_numeric,
        errors="coerce"
    )

    # Calculate percentage of pin
    df1['PiN_percentage'] = (df1['Final PiN'] / df1['Population']).where(df1['Population'] != 0)

    # Create the merge key on the pin sheet
    df1 = set_merge_key(df1)

    # Process the second sheet (Severity)
    df2 = pd.read_excel(input_file, sheet_name=1, header=None)
    df2 = set_header(df2)
    df2 = set_merge_key(df2)

    # Keep "Final Severity" column from the severity sheet
    df2 = df2[['merge_key', 'Final Severity'] + SECTOR_LIST]

    # Merge sheets using merge_key
    merged_df = pd.merge(
        df1,
        df2,
        on='merge_key',
        how='left',
        suffixes=('_pin', '_severity')
    )

    def make_sector_dict(row):
        return {
            s: {
                'pin': None if pd.isna(row.get(f'{s}_pin')) else row[f'{s}_pin'],
                'severity': None if pd.isna(row.get(f'{s}_severity')) else row[f'{s}_severity']
            }
            for s in SECTOR_LIST
        }

    merged_df['sectors'] = merged_df.apply(make_sector_dict, axis=1)

    drop_cols = [f'{s}_pin' for s in SECTOR_LIST] + [f'{s}_severity' for s in SECTOR_LIST]
    merged_df = merged_df.drop(columns=drop_cols)

    merged_df = merged_df.replace({np.nan: None})
    records = merged_df.to_dict(orient='records')

    # Create json output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=4, ensure_ascii=False)

    return merged_df


if __name__ == '__main__':
    input_path = 'input/GLOBAL-JIAF-DATA 1.04.2025.xlsx'
    output_path = 'output/jiaf2.json'
    result_df = process_data(input_path, output_path)
