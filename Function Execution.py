import csv
import pysolr
import subprocess

# Connect to your Solr instance
SOLR_URL = 'http://localhost:8983/solr'
solr = pysolr.Solr(SOLR_URL, always_commit=True)

# Define collection names based on your name and phone number
YOUR_NAME = "Suryaeswaran"  # e.g., "John Doe"
LAST_FOUR_DIGITS = "6317"  # e.g., "1234"
v_nameCollection = f'Hash_{YOUR_NAME.replace(" ", "_")}'  # Hash_Suryaeswaran
v_phoneCollection = f'Hash_{LAST_FOUR_DIGITS}'  # Hash_6317

def createCollection(collection_name):
    """Creates a new collection in Solr."""
    try:
        subprocess.run(['solr', 'create_collection', '-c', collection_name], check=True)
        print(f"Collection '{collection_name}' created successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while creating collection '{collection_name}': {e}")
    except FileNotFoundError:
        print("Solr command not found. Ensure that Solr is properly installed and added to PATH.")
    except PermissionError:
        print("Permission denied. Try running the script with elevated privileges.")

def index_csv_data(csv_file, p_collection_name, p_exclude_column):
    """Indexes the CSV data into Solr collection, excluding the specified column."""
    # Create a list to store employee records
    employee_data = []

    # Read the CSV file with a different encoding
    with open(csv_file, mode='r', newline='', encoding='ISO-8859-1') as file:
        reader = csv.DictReader(file)

        # Process each row in the CSV file
        for row in reader:
            # Remove the excluded column if it exists in the row
            row.pop(p_exclude_column, None)
            employee_data.append(row)

    # Create a Solr instance for the specific collection
    solr_core = pysolr.Solr(f'{SOLR_URL}/{p_collection_name}', always_commit=True)

    # Index the data into Solr
    solr_core.add(employee_data)
    print(f"Indexed data from {csv_file} into collection: {p_collection_name}")

def indexData(p_collection_name, p_column_name):
    """Indexes data based on a specific column name."""
    # Define a sample data format for indexing
    sample_data = [
        {"Employee_ID": "E001", "Gender": "Female"},
        {"Employee_ID": "E002", "Gender": "Male"},
        {"Employee_ID": "E003", "Gender": "Female"},
    ]
    
    # Create a Solr instance for the specific collection
    solr_core = pysolr.Solr(f'{SOLR_URL}/{p_collection_name}', always_commit=True)

    # Index the sample data into Solr
    solr_core.add(sample_data)
    print(f"Indexed sample data into '{p_column_name}' of collection: {p_collection_name}")

def getEmpCount(p_collection_name):
    """Fetches the count of employee records from the specified collection."""
    try:
        solr_core = pysolr.Solr(f'{SOLR_URL}/{p_collection_name}', always_commit=True)
        response = solr_core.search('Employee_ID:[* TO *]', **{'rows': 0})
        count = response.hits  # Number of documents found
        print(f"Total valid Employee ID records in '{p_collection_name}': {count}")
        return count
    except Exception as e:
        print(f"An error occurred while fetching the employee count: {e}")
        return 0

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    """Searches for records in the collection where the column matches the specified value."""
    query = f'{p_column_name}:{p_column_value}'
    solr_core = pysolr.Solr(f'{SOLR_URL}/{p_collection_name}', always_commit=True)

    try:
        results = solr_core.search(query)
        print(f"Found {len(results)} results for {p_column_name} = {p_column_value} in collection: {p_collection_name}")
        
        for result in results:
            print(result)
    
    except Exception as e:
        print(f"An error occurred while searching: {e}")

def delEmpById(p_collection_name, p_employee_id):
    """Deletes an employee from the collection by employee ID."""
    solr_core = pysolr.Solr(f'{SOLR_URL}/{p_collection_name}', always_commit=True)

    try:
        solr_core.delete(id=p_employee_id)
        print(f"Deleted employee with ID '{p_employee_id}' from collection: {p_collection_name}")
    except Exception as e:
        print(f"An error occurred while deleting employee ID '{p_employee_id}': {e}")

def getDepFacet(p_collection_name):
    """Retrieves employee count grouped by department from the specified collection."""
    solr_core = pysolr.Solr(f'{SOLR_URL}/{p_collection_name}', always_commit=True)

    try:
        facet_field = 'Department'
        
        response = solr_core.search('*:*', **{
            'facet': 'true',
            'facet.field': facet_field,
            'facet.mincount': 1,
            'rows': 0
        })

        if 'facets' in response:
            facet_counts = response.facets.get(facet_field, {})
            if facet_counts:
                print(f"Employee count grouped by department in '{p_collection_name}':")
                for department, count in facet_counts.items():
                    print(f"{department}: {count}")
            else:
                print(f"No facet counts found for '{facet_field}' in '{p_collection_name}'.")
        else:
            print(f"No facets found in response for '{p_collection_name}'.")
    except Exception as e:
        print(f"An error occurred while retrieving department facets: {e}")

if __name__ == "__main__":
    csv_file_path = r'C:\Users\Surya\Downloads\Employee Data.csv'


    # Index data excluding the 'Bonus %' column
    index_csv_data(csv_file_path, v_nameCollection, 'Bonus %')

    # Get the count of employees in v_nameCollection
    employee_count_name = getEmpCount(v_nameCollection)
    print(f"Total employee records in '{v_nameCollection}': {employee_count_name}")

    # Get the count of employees in v_phoneCollection
    employee_count_phone = getEmpCount(v_phoneCollection)
    print(f"Total employee records in '{v_phoneCollection}': {employee_count_phone}")

    # Index sample data based on the 'Department' column
    try:
        indexData(v_nameCollection, 'Department')
    except pysolr.SolrError as e:
        print(f"Error indexing data in {v_nameCollection}: {e}")

    # Index sample data based on the 'Gender' column
    try:
        indexData(v_phoneCollection, 'Gender')
    except pysolr.SolrError as e:
        print(f"Error indexing data in {v_phoneCollection}: {e}")

    # Example search for a specific employee by department in v_nameCollection
    searchByColumn(v_nameCollection, 'Department', 'IT')

    # Example search for male employees in v_nameCollection
    searchByColumn(v_nameCollection, 'Gender', 'Male')

    # Example search for employees by department in v_phoneCollection
    searchByColumn(v_phoneCollection, 'Department', 'IT')

    # Delete an employee by ID
    delEmpById(v_nameCollection, 'E02003')

    # Get department facets in v_nameCollection
    getDepFacet(v_nameCollection)

    # Get department facets in v_phoneCollection
    getDepFacet(v_phoneCollection)
