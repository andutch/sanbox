import os
import zipfile

# Function to recursively unzip files
def unzip_files(base_dir):
    processed_years = set()

    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.zip'):
                # Extract year and quarter from the file name
                year = file[:4]
                quarter = file[4:6]  # Correct index for the quarter
                quarter_folder_name = quarter + '_f345'  # Append '_f345' to the quarter folder name

                # Print processing of the year if not already processed
                if year not in processed_years:
                    print(f"Processing year: {year}")
                    processed_years.add(year)

                # Print processing of the quarter
                print(f"  Processing quarter: {quarter_folder_name} in year {year}")

                # Create the directory structure if it doesn't exist
                year_dir = os.path.join(base_dir, year)
                quarter_dir = os.path.join(year_dir, quarter_folder_name)
                os.makedirs(quarter_dir, exist_ok=True)

                # Full path to the zip file
                zip_file_path = os.path.join(root, file)

                # Unzip the file
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(quarter_dir)

                # Rename the unzipped files
                for unzipped_file in os.listdir(quarter_dir):
                    old_file_path = os.path.join(quarter_dir, unzipped_file)
                    new_file_path = os.path.join(quarter_dir, unzipped_file.replace('_form345', '_f345'))
                    os.rename(old_file_path, new_file_path)

# Define the base directory where the zip files are located
base_dir = '../EDGAR_form345'

# Call the function
unzip_files(base_dir)
