import pandas as pd
import requests
from sqlalchemy import create_engine
from spellchecker import SpellChecker
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import re

def load_csv(file_path):
    return pd.read_csv(file_path)

def load_excel(file_path):
    return pd.read_excel(file_path)

def load_api(api_url):
    #NOT APPLICABLE HERE, BUT ALSO INCLUDE THE PARAMETER TO HANDLE HEADERS, BCZ IN SOME CASES IT IS REQUIRED
    # headers = {
    #     'Authorization': 'Basic c3ZjLUFIU3N0YXJzLVZHRC1ROkhjMydkKiJkRDBkYVY9eD1tcj9HZ0NLfDg=',
    #     'Content-Type': 'application/json'
    # }

    # Added the try except block to handle potential issues, also added the timeout just to ensure the requests wait for 10 secs before throwing timeout error
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # Extract the 'data' list from the JSON response
            if 'data' in data:
                return pd.DataFrame(data['data'])
            else:
                print("Unexpected response structure")
                return None
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
            return None
    except requests.exceptions.Timeout:
        print("The request timed out")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def load_database(connection_string, query):
    engine = create_engine(connection_string)
    return pd.read_sql(query, engine)

def clean_data(data):
    # Remove duplicates
    data = data.drop_duplicates()

    # Remove null values or handle them appropriately
    data = data.dropna()

    # Correct spelling mistakes
    spell = SpellChecker()
    for col in data.select_dtypes(include=[object]).columns:
        data[col] = data[col].apply(
            lambda x: ' '.join(
                [spell.correction(word) if spell.correction(word) is not None else word for word in x.split()]
            ) if pd.notnull(x) else x
        )

    # Remove unwanted characters
    for col in data.select_dtypes(include=[object]).columns:
        data[col] = data[col].apply(
            lambda x: re.sub(r'[^A-Za-z0-9\s]+', '', x) if pd.notnull(x) else x
        )

    return data

def transform_data(data):
    # Example transformation: Standard scaling of numerical columns
    scaler = StandardScaler()
    numerical_cols = data.select_dtypes(include=[float, int]).columns
    data[numerical_cols] = scaler.fit_transform(data[numerical_cols])

    # Example transformation: One-hot encoding of categorical columns
    encoder = OneHotEncoder(sparse_output=False)  # Updated parameter name
    categorical_cols = data.select_dtypes(include=[object]).columns
    encoded_data = pd.DataFrame(encoder.fit_transform(data[categorical_cols]), columns=encoder.get_feature_names_out(categorical_cols))
    data = data.drop(categorical_cols, axis=1)
    data = pd.concat([data, encoded_data], axis=1)

    return data

def sort_data(data, column_name, ascending=True):
    return data.sort_values(by=column_name, ascending=ascending)

def check_domain_constraints(data, column_name, valid_values):
    invalid_values = data[~data[column_name].isin(valid_values)]
    if not invalid_values.empty:
        print(f"Invalid values found in column '{column_name}':")
        print(invalid_values)
    else:
        print(f"No domain constraint problems found in column '{column_name}'.")

def perform_analysis(data):
    # Clean the data
    cleaned_data = clean_data(data)

    # Example: Sort data by a specific column (replace 'name' with an actual column name if necessary)
    if 'name' in cleaned_data.columns:
        cleaned_data = sort_data(cleaned_data, 'name')

    # Example: Check for domain constraint problems (update 'column_name' and 'valid_values' with actual values)
    if 'some_column' in cleaned_data.columns:  # Replace 'some_column' with the actual column name you want to check
        valid_values = ['Value1', 'Value2', 'Value3']  # Replace with actual valid values
        check_domain_constraints(cleaned_data, 'some_column', valid_values)

    # Transform the data
    transformed_data = transform_data(cleaned_data)

    return cleaned_data, transformed_data


# if __name__ == "__main__":
#     import sys

#     if len(sys.argv) < 3:
#         print("Usage: python file.py <data_source> <source_path_or_url>")
#         sys.exit(1)

#     data_source = sys.argv[1]
#     source_path_or_url = sys.argv[2]

#     if data_source == 'csv':
#         data = load_csv(source_path_or_url)
#     elif data_source == 'excel':
#         data = load_excel(source_path_or_url)
#     elif data_source == 'api':
#         data = load_api(source_path_or_url)
#     elif data_source == 'database':
#         if len(sys.argv) < 4:
#             print("Usage for database: python file.py database <connection_string> <query>")
#             sys.exit(1)
#         query = sys.argv[3]
#         data = load_database(source_path_or_url, query)
#     else:
#         print("Unsupported data source")
#         sys.exit(1)

#     cleaned_data, transformed_data = perform_analysis(data)

#     # Save the cleaned data to a new file
#     cleaned_data.to_csv('cleaned_data.csv', index=False)

#     # Save the transformed data to a new file
#     transformed_data.to_csv('transformed_data.csv', index=False)
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python file.py <data_source> <source_path_or_url>")
        sys.exit(1)

    data_source = sys.argv[1]
    source_path_or_url = sys.argv[2]

    data = None

    if data_source == 'csv':
        data = load_csv(source_path_or_url)
    elif data_source == 'excel':
        data = load_excel(source_path_or_url)
    elif data_source == 'api':
        data = load_api(source_path_or_url)
    elif data_source == 'database':
        if len(sys.argv) < 4:
            print("Usage for database: python file.py database <connection_string> <query>")
            sys.exit(1)
        query = sys.argv[3]
        data = load_database(source_path_or_url, query)
    else:
        print("Unsupported data source")
        sys.exit(1)

    if data is None:
        print("Failed to load data. Exiting.")
        sys.exit(1)

    cleaned_data, transformed_data = perform_analysis(data)

    # Save the cleaned data to a new file
    cleaned_data.to_csv('cleaned_data.csv', index=False)

    # Save the transformed data to a new file
    transformed_data.to_csv('transformed_data.csv', index=False)