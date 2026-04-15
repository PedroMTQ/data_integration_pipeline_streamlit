This is a "temp" service (until we standardize it with  the rest  of the workflow).
This is  a multi-step workflow:
 1. Reads data from the delta tables and upserts the data to a relational DB
 2. Reads data from the content extractor DB and upserts to the relational DB 
 3. Takes the data from the relational DB and inserts it into elastic search