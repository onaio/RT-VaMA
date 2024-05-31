---A database contains one or more schemas which in turn contain one or more tables.
-- There is no standard way of naming your schemas however, it is recommended to use names that relate to either the tables origin or the views/tables destination
--- Prior to developing the scripts, connectors to push data from Inform to the database had to be created. Once the tables were available in the database, the scripts were developed using Structured Query Language (SQL).
---The scripts are hosted in github and can be developed using your preferred SQL editor tool. 
-- Within the RT-VaMA database, there are 5 schemas created using the sql queries below. For more information on schemas take a look at this guide (https://www.postgresql.org/docs/current/ddl-schemas.html)

---Creates the templates schema to host the templates from Inform
create schema if not exists templates;

---Creates the CSV schema to host the location templates and any CSV file that is uploaded to the database 
create schema if not exists csv;

---Creates the schema that hosts the final views for visualization
create schema if not exists reporting;


---Creates a staging schema that hosts the views that have labels from the registry table and views that can be joined to create a final view for visualization
create schema if not exists staging;

---Creates an airbyte_removed_group schema that hosts airbyte tables that have group names removed
create schema if not exists airbyte_removed_group;