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
participant_info_2_include = ["Row","ID1","LastName","FirstName","Age","Heightcm","Weightkg","TestTime","TestDuration","ExerciseDuration", 
                              "SpirometryFVCPre","SpirometryFVCPred","SpirometryFVCpPred","SpirometryFEV1Pre",
                              "SpirometryFEV1Pred","SpirometryFEV1pPred","SpirometryMVVPre",
                              "ProtocoltAT","ProtocoltRC","ProtocoltMax","ProtocolPowerWarmUp","ProtocolPowerAT",
                              "ProtocolPowerRC","ProtocolPowerMax","ProtocolRevolutionWarmUp","ProtocolRevolutionAT",
                              "ProtocolRevolutionRC","ProtocolRevolutionMax",
                              "MetabolicVO2Rest","MetabolicVO2WarmUp","MetabolicVO2AT","MetabolicVO2RC","MetabolicVO2Max","MetabolicVO2Pred",
                              "MetabolicVO2pPred","MetabolicVO2_KgRest","MetabolicVO2_KgWarmUp","MetabolicVO2_KgAT","MetabolicVO2_KgRC",
                              "MetabolicVO2_KgMax","MetabolicVO2_KgPred","MetabolicVO2_KgpPred","MetabolicMETSRest","MetabolicMETSWarmUp",
                              "MetabolicMETSAT","MetabolicMETSRC","MetabolicMETSMax","MetabolicMETSPred","MetabolicMETSpPred","MetabolicRQRest",
                              "MetabolicRQWarmUp","MetabolicRQAT","MetabolicRQRC","MetabolicRQMax",
                              "VentilatoryVE_VCO2slopeMeas","VentilatoryVE_VCO2slopePred","VentilatoryVE_VCO2slopepPred",
                              "VentilatoryVE_VCO2intercMeas","VentilatoryOUESMeas","VentilatoryVERest","VentilatoryVEWarmUp",
                              "VentilatoryVEAT","VentilatoryVERC","VentilatoryVEMax","VentilatoryBRAT","VentilatoryBRRC",
                              "VentilatoryBRMax","VentilatoryVTRest","VentilatoryVTWarmUp","VentilatoryVTAT","VentilatoryVTRC",
                              "VentilatoryVTMax","VentilatoryRfRest","VentilatoryRfWarmUp","VentilatoryRfAT","VentilatoryRfRC",
                              "VentilatoryRfMax","CardiovascularHRRest","CardiovascularHRWarmUp","CardiovascularHRAT",
                              "CardiovascularHRRC","CardiovascularHRMax","CardiovascularHRPred","CardiovascularHRpPred",
                              "CardiovascularHRRMeas","CardiovascularHRR_1_minuteMeas","CardiovascularVO2_WRSlopeMeas",
                              "CardiovascularVO2_WRSlopePred","CardiovascularVO2_WRSlopepPred","CardiovascularVO2_HRRest",
                              "CardiovascularVO2_HRWarmUp","CardiovascularVO2_HRAT","CardiovascularVO2_HRRC","CardiovascularVO2_HRMax",
                              "CardiovascularVO2_HRPred","CardiovascularVO2_HRpPred","CardiovascularPSystRest","CardiovascularPSystWarmUp",
                              "CardiovascularPDiastRest","CardiovascularPDiastWarmUp","GasExchangeVO2_ATMeas","GasExchangePetCO2Rest",
                              "GasExchangePetCO2WarmUp","GasExchangePetCO2AT","GasExchangePetCO2RC","GasExchangePetCO2Max",
                              "GasExchangePetO2Rest","GasExchangePetO2WarmUp","GasExchangePetO2AT","GasExchangePetO2RC","GasExchangePetO2Max",
                              "GasExchangeVE_VO2AT","GasExchangeVE_VO2RC","GasExchangeVE_VO2Max","GasExchangeVE_VCO2AT","GasExchangeVE_VCO2RC",
                              "GasExchangeVE_VCO2Max","GasExchangeVE_VCO2Pred","GasExchangeVE_VCO2pPred","GasExchangeSpO2Rest","GasExchangeSpO2WarmUp",
                              "GasExchangeSpO2AT","GasExchangeSpO2RC","GasExchangeSpO2Max"]

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
  #print(dataframe)
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
  
  for i in range(len(rows)):
    split_dataheaders.append(dataframe.iloc[rows[i]][0])
    if i < len(rows) - 1:
      current_dataframe = dataframe.iloc[rows[i]+2:rows[i+1]-1]
    else:
      current_dataframe = dataframe.iloc[rows[i]+2:]
    
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

  
  merged_data_df = pd.DataFrame(columns=participant_info_2_include)

  

  for i in range(len(final_list)):
    participant_data = {}
    file=final_list[i]

    # finding the patient id by traversing from leaf to the parent nodes
    grandpa_directory = os.path.dirname(os.path.dirname(file))
    patient_id = os.path.basename(grandpa_directory)
    participant_data["Row"] = patient_id

    # get the datasheet
    for label_value_pair in ["A:B","D:E","G:H"]:
      data = pd.read_excel(file, usecols=label_value_pair, header=None)
      data.columns=["label","value"]
      data = data.dropna()
      for index, row in data.iterrows():
        value = row["value"]
        
        if isinstance(value, str):
          if value == "-" or value == "None" or value.lower() == "convalescence" or value.lower == "covalescence":
            value = ""
        
        label = re.sub(r'\s', '', row["label"])
        label = label.replace("(","")
        label = label.replace(")","")
        
        if label in participant_info_2_include:
          participant_data[label] = value
        

    # get the results sheet
    dataframe = pd.read_excel(file, sheet_name="Results", header=None)

    # find rows with a string at location 0, and the rest of the columns is NaN      
    indexes = find_rows_with_strings_at_location_0(dataframe)
    #print(indexes)

    split_dataheaders, split_dataframes = split_dataframe_by_rows(dataframe, indexes)
    
    
    #print(patient_id, len(split_dataframes))
    
    for i in range(len(split_dataframes)):
      subset_data = split_dataframes[i]
      
      for index, row in subset_data.iterrows():
        parameter_name = row["Parameter"]
        uom = row["um"]
        for column, value in row.iteritems():
            if column not in ["Parameter", "um"]:
                if '%' in column:
                    key_name = split_dataheaders[i] + parameter_name + column
                    key_name = key_name.replace("%","p")
                else:
                    key_name = split_dataheaders[i] + parameter_name + column
                key_name = key_name.replace("/","_")
                key_name = key_name.replace("@","_")
                key_name = key_name.replace(".","")

                # remove all white space
                key_name = re.sub(r'\s', '', key_name)

                # in this code we extract only numeric data
                if isinstance(value, str):
                  value = None
                
                if key_name != None and key_name in participant_info_2_include:
                    participant_data[key_name] = value
                    
    merged_data_df = merged_data_df.append(participant_data, ignore_index=True)

  merged_data_df.to_csv(output, index=False)

  

if __name__ == "__main__":
  main()
