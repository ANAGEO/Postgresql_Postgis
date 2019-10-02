'''
Function that import a CSV file into PostgreSQL
- 'con' parameter wait for a psycopg2 connexion object established to the postgresql database 
- 'csv' parameter wait for the path to the .csv file (string)
- 'column_definition' parameter wait for a list of tuple with the column name and datatype for the create table statement.
- 'schema' parameter wait for the name of the schema (string)
- 'table' parameter wait for the name of the table (string)
- 'delimiter' parameter wait for the csv delimiter character (string)
- 'null' parameter wait for the character representing null values in the csv (string)
- 'overwrite' parameter wait for a True/False value. If False, existing table in the database will not be removed and an error will occur
- Tricks found here https://stackoverflow.com/a/50034387
- Potential improvement: auto check for encoding compatibility with posgresql standards https://docs.postgresql.fr/8.3/multibyte.html
'''

import subprocess
import psycopg2
import os

def ImportCSV(con, csv, column_definition, schema, table, delimiter=";", null="", overwrite=False):
    try:
        ## Saving current time for processing time management
        begintime_copy=time.time()
        
        # Determine automatically the encoding of the file
        cmd = ['file', '-b', '--mime-encoding', data['cama'][1]]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr=p.communicate()
        csv_encoding = stdout.split("\n")[0]
        
        # If decoding and encoding to UTF-8 is needed
        if csv_encoding.lower() != 'utf-8':
            print ('The csv file is encoded in %s. A utf-8 copy will be used for copy in Postgresql.'%csv_encoding.lower())
            # Create a new file that is coded as UTF8 since the original .csv is coded as 'iso-8859-1' which is not 
            # compatible with PostgreSQL
            path,ext = os.path.splitext(csv)
            csv_tmp = os.path.join(tempfile.gettempdir(),'%s_utf8%s'%(os.path.split(path)[-1],ext))
            fin = open(csv_tmp, 'w')
            with open(csv, 'r') as f:
                for row in f:
                    # Write row decode from user specified encoding and encoded to 'utf8'
                    fin.write(row.decode(csv_encoding).encode('utf8'))
            fin.close()
            csv = csv_tmp      
        
        ## CHECK IF TABLE EXISTS IN THE SCHEMA !!! 
        # Print
        print "Creating new table copy csv file in the postgresql table"
        # Create cursor
        cursor = con.cursor()
        # Create table query
        query="DROP TABLE IF EXISTS %s.%s;"%(schema,table)
        query+="CREATE TABLE %s.%s ("%(schema,table)
        query+=", ".join(['%s %s'%(column_name, column_type) for column_name, column_type in column_definition])
        query+=");"

        # Print the query
        print query
        # Execute the CREATE TABLE query 
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        
        # Print
        print "Start copy csv file in the postgresql table"       
        # Psycopg2 COPY FROM function 
        with open(csv, 'r') as f:
            next(f)  # Skip the header row.
            #Clean the content of the file - Ensure there is only one newline return (\n) and no carriage return (\r) 
            content = StringIO('\n'.join(line.split("\n")[0].replace("\r","") for line in f)) 
            cursor.copy_from(content, '%s.%s'%(schema,table), sep=delimiter, null=null)
        # Make the changes to the database persistent
        con.commit()    
        # Close connection with database
        cursor.close()
        # Remove temporary file if needed
        if csv_encoding.lower() != 'utf-8':
            os.remove(csv)
        ## Compute processing time and print it
        print print_processing_time(begintime_copy, "Process achieved in ")
        
    except (Exception, psycopg2.Error) as error:
        sys.exit(error)