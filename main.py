import pandas as pd

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

    # Keep columns ISO3, Population, Final PiN, and any column beginning with "Admin"
    cols_to_keep = [col for col in df1.columns if
                    (str(col).startswith("Admin")) or col in [
                        "ISO3",
                        "Population",
                        "CCCM",
                        "Education",
                        "Nutrition",
                        "Food Security",
                        "Health",
                        "Protection",
                        "Shelter",
                        "WASH",
                        "General Protection",
                        "Child Protection",
                        "GBV",
                        "Mine Action",
                        "HLP",
                        "Final PiN"
                    ]]
    df1 = df1[cols_to_keep]

    # convert cols to numeric
    exclude_mask = df1.columns.str.startswith("Admin") | (df1.columns == "ISO3")
    to_numeric_cols = df1.columns[~exclude_mask]
    df1[to_numeric_cols] = df1[to_numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Calculate percentage of pin
    df1['PiN_percentage'] = (df1['Final PiN'] / df1['Population']).where(df1['Population'] != 0)

    # Create the merge key on the pin sheet
    df1 = set_merge_key(df1)

    # Process the second sheet (Severity)
    df2 = pd.read_excel(input_file, sheet_name=1, header=None)
    df2 = set_header(df2)
    df2 = set_merge_key(df2)

    # Keep "Final Severity" column from the severity sheet
    df2 = df2[['merge_key', 'Final Severity']]

    # Merge sheets using merge_key
    merged_df = pd.merge(df1, df2, on='merge_key', how='left')

    # Create json output
    json_data = merged_df.to_json(orient='records', indent=4, force_ascii=False)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(json_data)

    return merged_df


if __name__ == '__main__':
    input_path = 'input/GLOBAL-JIAF-DATA 1.04.2025.xlsx'
    output_path = 'output/output.json'
    result_df = process_data(input_path, output_path)
