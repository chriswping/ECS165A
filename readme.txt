Weidong Guo (998013837)

Christopher Ng (998311403)



Python version: 2.7.10


Instructions: 


Make sure postgres is started via start_postgres - you can check this with psql postgres

Make sure the postgres database is empty with \dt

If it's not empty type DROP TABLE <table_name>; for each table

If you're still in postgress type \q to exit back to the terminal command line



Run insert.py by typing:



python insert.py



Exactly as is and wait for tables to load. 
You will see a "$" when this is done.



Now that the tables are inserted, query.py will handle all the queries; type:



python query.py 



This will display a menu which will allow you to press a number (1-7) corresponding
to the query on the screen, or to quit:

‘1’ corresponds to 3a
‘2’ corresponds to 3b
‘3’ corresponds to 3c
‘4’ corresponds to 3d
‘5’ corresponds to 5a
‘6’ corresponds to 5b
‘7’ quits
    


