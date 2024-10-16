import configparser
import mysql.connector
def connect_to_mysql():
    """ Connects to the the MySQL server and returns a cursor object."""
    # Read the database configuration from the .ini file.
    config = configparser.ConfigParser()
    config.read('database.ini')

    db_config = {
        'host' : config['mysql']['host'],
        'user' : config['mysql']['user'],
        'password' : config['mysql']['password'],
        'database' : config['mysql']['database']

    }
    
    try:

        # Connect to the MySQL server
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        print("Connected to MySQL server successfully.")
        return cursor, connection
    except mysql.connector.Error as error:
        print(f"Error connecting to MySQL server: {error}")
        exit(1)

def create_database(cursor, connection, database_name):
    """ Creates a database with the given name."""
    try:
        cursor.execute(f"CREATE DATABASE {database_name}")
        connection.commit()
        print(f"Database '{database_name}' created successfully.")
    except mysql.connector.Error as error:
        print(f"Error creating database: {error}")

def create_table(cursor, connection, database_name, table_name, columns):
    """ Creates a table with the given name and columns."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        column_sql = ",".join([f"{column} VARCHAR(255)" for column in columns])
        cursor.execute(f"CREATE TABLE {table_name}({column_sql})")
        connection.commit()
        print(f"Table '{table_name}' created successfully in database '{database_name}'.")
    except mysql.connector.Error as error:
        print(f"Error creating table: {error}")

def list_tables(cursor, connection, database_name):
    """ Prints the names of tables in the given database."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        if tables:
            print("Tables in database {database_name}: ")
            for table in tables:
                print(table[0])

        else:
            print(f"No tables found in database '{database_name}'.")
    except mysql.connector.Error as error:
        print("Error listing tables: {error}")

def insert_into_table(cursor, connection, database_name, table_name, column_names, values):
    """ Inserts values into a specific table in a specific database."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()

        # Construct the SQL INSERT statement with placeholders
        sql = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES({','.join(['%s'] * len(column_names))})"
        # Execute the SQL statement with the values
        cursor.execute(sql, values)
        connection.commit()
        print(f"Data inserted into table '{table_name}' in database '{database_name}' successfully.")
    except mysql.connector.Error as error:
        print(f"Error inserting data: {error}")

def list_databases(cursor):
    """ Prints the names of existing databases."""
    try:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        if databases:
            print("Existing databases: ")
            for database in databases:
                print(database[0])
        else:
            print("No databases found.")
    except mysql.connector.Error as error:
        print("Error listing databases: {error}")

def drop_database(cursor, connection, database_name):
    """ Drops the specified database."""
    try:
        cursor.execute(f"DROP DATABASE {database_name}")
        connection.commit()
        print(f"Database '{database_name}' dropped successfully.")
    except mysql.connector.Error as error:
        print("Error dropping database: {error}")

def drop_table(cursor, connection, database_name, table_name):
    """" Drops the specified table from the specified database."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        cursor.execute(f"DROP TABLE {table_name}")
        connection.commit()
        print(f"Table '{table_name}' dropped successfully from database '{database_name}'.")
    except mysql.connector.Error as error:
        print("Error dropping table: {error}")

def list_columns(cursor, connection, database_name, table_name):
    """ Lists the columns of a specific table in a specific database."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        if columns:
            print(f"Columns in table '{table_name}' in '{database_name}': ")
            for column in columns:
                print(column[0])    # Print only the column name
        else:
            print(f"No columns found in table '{table_name}' in database '{database_name}'. ")
    except mysql.connector.Error as error:
        print(f"Error listing columns: {error}")

def delete_row(cursor, connection, database_name, table_name, where_clause):
    """ Deletes a row from a specific table in a specific database."""
    try: 
        cursor.execute(f"USE {database_name}")
        connection.commit()
        sql = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(sql)
        connection.commit()
        print(f"Row deleted from table '{table_name}' in database '{database_name}'.")
    except mysql.connector.Error as error:
        print(f"Error deleting row: {error}")  

def select_all_records(cursor, connection, database_name, table_name):
    """ Select all records from a specific table in a specific database."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        cursor.execute(f"SELECT * FROM {table_name}")
        records = cursor.fetchall()
        if records:
            print("Records from table '{table_name}' in database '{database_name}':")
            for record in records:
                print(record)   # Print the entire row as a tuple
        else:
            print("No records found in table '{table_name}' in database '{database_name}'.")
    except mysql.connector.Error as error:
        print(f"Error selecting records: {error}")

def select_records_with_where_clause(cursor, connection, database_name, table_name, where_clause):
    """ Selects records from a specific table in a specific database based on a WHERE clause."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
        cursor.execute(sql)
        records = cursor.fetchall()
        if records:
            print(f"Records from table '{table_name}' in database '{database_name}' matching the WHERE clause: ")
            for record in records:
                print(record)   # Print the entire row as a tuple
        else:
            print(f"No records found in table '{table_name}' in database '{database_name}'matching the WHERE clause.")
    except mysql.connector.Error as error:
        print(f"Error selecting records: {error}")

def select_sorted_records(cursor, connection, database_name, table_name, sort_column, sort_order = 'ASC'):
    """ Select all records from a specific table in a specific database, sorted by a specified column."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        sql = f"SELECT * FROM {table_name} ORDER BY {sort_column} {sort_order}"
        cursor.execute(sql)
        records = cursor.fetchall()
        if records:
            print(f"Sorted records from table '{table_name}' in database '{database_name}': ")
            for record in records:
                print(record)   # Print the entire row as a tuple
        else:
            print(f"No records found in table '{table_name}' in database '{database_name}'.")
    except mysql.connector.Error as error:
        print(f"Error selecting sorted records: {error}")

def update_record(cursor, connection, database_name, table_name, column_name, new_value, where_condition):
    """ Updates a record in a table in a specific database."""
    try:
        cursor.execute(f"USE {database_name}")
        connection.commit()
        sql = f"UPDATE {table_name} SET {column_name} = %s WHERE {where_condition}"
        cursor.execute(sql, (new_value,))
        connection.commit()
        rows_affected = cursor.rowcount
        print(f"{rows_affected} rows updated in table '{table_name}' in database '{database_name}'.")
    except mysql.connector.Error as error:
        print(f"Error updating record: {error}")  

def create_user(cursor, connection, new_username, new_password, database_name, privileges):
    """ Creates a new user with access to a specific database and with specified privileges."""
    try:
        cursor.execute(f"CREATE USER '{new_username}'@'localhost' IDENTIFIED BY '{new_password}'")
        connection.commit()
        cursor.execute(f"GRANT {privileges} ON {database_name}.* TO '{new_username}'@'localhost'")
        connection.commit()
        print(f"User '{new_username}' created with {privileges} privileges on database '{database_name}'.")
    except mysql.connector.Error as error:
        print(f"Error creating user: {error}")

def drop_user(cursor, connection, username):
    """ Drops an existing user from the MySQL server."""
    try:
        cursor.execute(f"DROP USER '{username}'@'localhost'")
        connection.commit()
        print(f"Uer '{username}' dropped successfully.")
    except mysql.connector.Error as error:
        print(f"Error dropping user: {error}")

#################################
def main():
    """ Main function for interacting with the MySQL server. """
    cursor , connection = connect_to_mysql()

    while True:
        print("\nChoose an action:")
        print("1. Create Database")
        print("2. Create Table")
        print("3. List Tables")
        print("4. Insert into Table")
        print("5. List Databases")
        print("6. Drop Database")
        print("7. Drop Table")
        print("8. List Columns")
        print("9. Delete Row")
        print("10. Select All Records")
        print("11. Select Records with WHERE clause")
        print("12. Select Sorted Recods")
        print("13. Update Record")
        print("14. Create User")
        print("15. Drop User")
        print("16. Exit")

        choice = input("Enter your choice (1-16): ")
        if choice == "1":
            database_name = input("Enter database name: ")
            create_database(cursor, connection, database_name)
        elif choice == "2":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name: ")
            columns = input("Enter columns seperated by commas (e.g. , column1, column2 ):").split(",")
            create_table(cursor, connection, database_name, table_name, columns)
        elif choice == "3":
            database_name = input("Enter database name: ")
            list_tables(cursor, connection, database_name)
        elif choice == "4":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name: ")
            column_names = input("Enter columns seperated by commas (e.g. , column1, column2 ):").split(",")
            values = input("Enter values separated by commas (e.g., value1, value2):").split(",")
            insert_into_table(cursor, connection, database_name, table_name, column_names, values)
        elif choice == "5":
            list_databases(cursor)
        elif choice == "6":
            database_name = input("Enter database name to drop: ")
            drop_database(cursor, connection, database_name)
        elif choice == "7":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name to drop: ")
            drop_table(cursor, connection, database_name, table_name)
        elif choice == "8":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name: ")
            list_columns(cursor, connection, database_name, table_name)
        elif choice == "9":
            database_name = input("Enter database name: ")
            table_name = input("Enter table_name: ")
            where_clause = input("Enter WHERE clause (e.g. 'id = 5'): ")
            delete_row(cursor, connection, database_name, table_name, where_clause)
        elif choice == "10":
            database_name = input("Enter database name: ")
            table_name = input("Enter table_name: ")
            select_all_records(cursor, connection, database_name, table_name)
            
        elif choice == "11":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name: ")
            where_clause = input("Enter WHERE clause (e.g., 'age > 30'): ")
            select_records_with_where_clause(cursor, connection, database_name, table_name, where_clause)
        elif choice == "12":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name: ")
            sort_column = input("Enter column to sort by: ")
            sort_order = input("Enter sort order (ASC or DESC, default is ASC): ").upper() or 'ASC'
            select_sorted_records(cursor, connection, database_name, table_name, sort_column, sort_order)

        elif choice == "13":
            database_name = input("Enter database name: ")
            table_name = input("Enter table name: ")
            column_name = input("Enter column to update: ")
            new_value = input("Enter new value: ")
            where_condition = input("Enter WHERE clause (e.g., 'id = 1'): ")
            update_record(cursor, connection, database_name, table_name, column_name, new_value, where_condition)
        elif choice == "14":
            new_username = input("Enter username: ")
            new_password = input("Enter password: ")
            database_name = input("Enter database name: ")
            privileges = input("Enter privileges (e.g., SELECT, INSERT, UPDATE, DELETE): ")
            create_user(cursor, connection, new_username, new_password, database_name, privileges)
        elif choice == "15":
            username = input("Enter username: ")
            drop_user(cursor, connection, username)
        elif choice == "16":
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 11.")
    cursor.close()
    connection.close()




if __name__ == "__main__":
    main()