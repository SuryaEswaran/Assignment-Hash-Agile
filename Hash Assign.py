import csv
import pysolr

# Connect to your Solr instance, using the specific core/collection
SOLR_URL = 'http://localhost:8983/solr/database_hash'  # Specify the core name here
solr = pysolr.Solr(SOLR_URL, always_commit=True)

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

    # Index the data into Solr
    solr.add(employee_data)
    print(f"Indexed data from {csv_file} into collection: {p_collection_name}")

def getEmpCount(p_collection_name):
    """Fetches the count of employee records from the specified collection."""
    try:
        # Execute a query to count valid employee ID records in the specified collection
        response = solr.search('Employee_ID:[* TO *]', **{'rows': 0})  # Adjust field name based on your schema
        count = response.hits  # Number of documents found
        print(f"Total valid Employee ID records in '{p_collection_name}': {count}")
        return count
    except Exception as e:
        print(f"An error occurred while fetching the employee count: {e}")
        return 0  # Return 0 in case of error

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    """Searches for records in the collection where the column matches the specified value."""
    # Construct the search query
    query = f'{p_column_name}:{p_column_value}'

    # Execute the search query
    try:
        results = solr.search(query)
        print(f"Found {len(results)} results for {p_column_name} = {p_column_value} in collection: {p_collection_name}")
        
        # Print each result
        for result in results:
            print(result)
    
    except Exception as e:
        print(f"An error occurred while searching: {e}")

def delEmpById(p_collection_name, p_employee_id):
    """Deletes an employee from the collection by employee ID."""
    try:
        solr.delete(id=p_employee_id)
        print(f"Deleted employee with ID '{p_employee_id}' from collection: {p_collection_name}")
    except Exception as e:
        print(f"An error occurred while deleting employee ID '{p_employee_id}': {e}")

def getDepFacet(p_collection_name):
    """Retrieves employee count grouped by department from the specified collection."""
    try:
        facet_field = 'Department'  # Ensure this matches your Solr schema
        
        response = solr.search('*:*', **{
            'facet': 'true',
            'facet.field': facet_field,
            'facet.mincount': 1,  # Only include counts greater than 0
            'rows': 0  # We only want the facet counts, not actual documents
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
    # Path to your CSV file
    csv_file_path = r'C:\Users\Surya\Downloads\Employee Data.csv'
    
    # Collection name (assumed from the previous part of the task)
    v_nameCollection = "database_hash"

    # Index data excluding the 'Bonus %' column
    index_csv_data(csv_file_path, v_nameCollection, 'Bonus %')

    # Execute the required functions in the specified order
    employee_count = getEmpCount(v_nameCollection)  # Count of employees
    print(f"Indexed {employee_count} records into the collection '{v_nameCollection}'.")

    # Example search for a specific employee by name
    searchByColumn(v_nameCollection, 'Full_Name', 'Kai Le')

    # Delete an employee by ID
    delEmpById(v_nameCollection, 'E02002')

    # Get department facets
    getDepFacet(v_nameCollection)
