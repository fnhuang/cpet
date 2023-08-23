import os
import pandas as pd
import argparse
import sys
import math
import re


  
# general parameters for the script
# --dir XXXXXXXX - where the parent folder of the data is kept
# --subproject YYYYYY - the subset of data ie CPET
# --config - json file to define table headers of interest and cells required for output

# configs
# directory = "./"
# subproject = "CPET"
validate_sheets = ["Data", "Results"]
# os.chdir('D:/Cloud/Onedrive/Work/UCL/Projects/LHA/Code')


## functions
def parse_args():
  """Parses the arguments passed to the script."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--dir',
      help='The data directory.',
      type=str,
      required=True)
  parser.add_argument(
      '--subproject',
      help='The subproiject name, i.e. "CPET"',
      type=str,
      required=True)
  parser.add_argument(
      '--output',
      help='The output file inc path.',
      type=str,
      required=True)
  return parser.parse_args()


def find_xlsx_files(directory):
  """Finds all .xlsx files under the specified directory recursively.

  Args:
    directory: The directory to search.

  Returns:
    A list of the full paths of all .xlsx files found.
  """
  xlsx_files = []
  for root, directories, files in os.walk(directory):
    for file in files:
      if file.endswith(".xlsx"):
        xlsx_files.append(os.path.join(root, file))
  return xlsx_files

def subset_on_project(list, subproject):
  """Subsets a list to return only those strings that contain the string "subproject".

  Args:
    list: The list to subset.

  Returns:
    A new list containing only the strings from the original list that contain
    the string 'subproject'.
  """
  new_list = []
  for string in list:
    if subproject in string:
      new_list.append(string)
  return new_list

def file_contains_all_sheets(xlsx_file, validate_sheets):
  worksheet_names = pd.ExcelFile(xlsx_file).sheet_names
  if all(sheet in worksheet_names for sheet in validate_sheets):
    return True
  else:
    return False



def find_rows_with_strings_at_location_0(dataframe):
  """Finds all the indexes in the DataFrame where the value at location 0 is a string and the rest of the row is NaN.

  Args:
    dataframe: The DataFrame to search.

  Returns:
    A list of the indexes where the value at location 0 is a string and the rest of the row is NaN.
  """

  rows = []
  for i in range(len(dataframe)):
    row = dataframe.iloc[i]
    if isinstance(row[0], str) and all(pd.isnull(value) for value in row[1:]):
      rows.append(i)
  return rows


def split_dataframe_by_rows(dataframe, rows):
  """Splits a Pandas DataFrame into smaller DataFrames based upon the rows variable.

  Args:
    dataframe: The DataFrame to split.
    rows: A list of the indexes to split the DataFrame at.

  Returns:
    A list of DataFrames, where each DataFrame contains the data from the rows variable to the next entry in rows-1.
  """

  split_dataframes = []
  split_dataheaders = []
  
  for i in range(len(rows)-1):
    split_dataheaders.append(dataframe.iloc[rows[i]][0])
    if i < len(rows) - 1:
      current_dataframe = dataframe.iloc[rows[i]+2:rows[i+1]-1]
    else:
      current_dataframe = dataframe.iloc[rows[i]:]
    
    current_dataframe.columns = list(dataframe.iloc[rows[i] + 1])
    current_dataframe = current_dataframe.reset_index(drop=True)

    
    include_indices = [i for i, col in enumerate(current_dataframe.columns) if not pd.isna(col)]
    # Remove columns with NaN header 
    current_dataframe = current_dataframe[current_dataframe.columns[include_indices]]
    # Drop the rows with all NaN entries from the dataframe. Drop non columns
    current_dataframe = current_dataframe.dropna(axis=0, how='all') 

  
    split_dataframes.append(current_dataframe)
    
  return split_dataheaders, split_dataframes

def main():
  print("Loading configuration...")
  args = parse_args()
  print("Arguments passed to script:")
 
  
  print("Current working directory:", os.getcwd())
  directory = args.dir
  subproject = args.subproject
  output= args.output
  validate_sheets = ["Data", "Results"]
  os.chdir(directory)
  
  # grab all XLSX files in parent directory, recursively
  xlsx_files = find_xlsx_files(directory)
  
  # subset on the project of interest
  new_list = subset_on_project(xlsx_files, subproject)
  final_list = [x for x in new_list if file_contains_all_sheets(x, validate_sheets)]
  print(f"file with valid sheets and under subproject {subproject} folder", final_list)

  
  merged_data_df = pd.DataFrame()

  

  for i in range(len(final_list)):
    participant_data = {}
    file=final_list[i]

    # finding the patient id by traversing from leaf to the parent nodes
    grandpa_directory = os.path.dirname(os.path.dirname(file))
    patient_id = os.path.basename(grandpa_directory)

    # get the datasheet
    for label_value_pair in ["A:B","D:E","G:H"]:
      data = pd.read_excel(file, usecols=label_value_pair, header=None)
      data.columns=["label","value"]
      data = data.dropna()
      for index, row in data.iterrows():
        value = row["value"]
        if value == "-" or value == "None":
          value = ""
        participant_data[row["label"]] = value

    # get the results sheet
    dataframe = pd.read_excel(file, sheet_name="Results")

    # find rows with a string at location 0, and the rest of the columns is NaN      
    indexes = find_rows_with_strings_at_location_0(dataframe)

    split_dataheaders, split_dataframes = split_dataframe_by_rows(dataframe, indexes)
    
    for i in range(len(split_dataframes)):
      subset_data = split_dataframes[i]
      for index, row in subset_data.iterrows():
        parameter_name = row["Parameter"]
        uom = row["um"]
        for column, value in row.iteritems():
            if column not in ["Parameter", "um"]:
                if '%' in column:
                    key_name = split_dataheaders[i] + parameter_name + column
                else:
                    key_name = split_dataheaders[i] + parameter_name + column + f"({uom})"

                # remove all white space
                key_name = re.sub(r'\s', '', key_name)

                # '-' is considered none
                if value == "-":
                    value = None
                
                if key_name != None:
                    participant_data[key_name] = value
    #print(participant_data)
    merged_data_df = merged_data_df.append(participant_data, ignore_index=True)
  
  merged_data_df.to_csv(output, index=False)

  

if __name__ == "__main__":
  main()
