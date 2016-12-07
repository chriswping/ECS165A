import psycopg2
import os ## for enviornment variable
import sys
import csv
import NHTS
import EIA

HOME_PATH = os.environ['HOME']
# database info
LOCAL_DB = "host=%s/postgres dbname=postgres port=5432" %(HOME_PATH)
# path to EIA csv files
EIA_PATH = "/home/cjnitta/ecs165a"
CO2_E_PATH = "%s/EIA_CO2_Electricity_2015.csv" %(EIA_PATH)
CO2_T_PATH = "%s/EIA_CO2_Transportation_2015.csv" %(EIA_PATH)
MKWH_PATH = "%s/EIA_MkWh_2015.csv" %(EIA_PATH)
#path to NHTS csv files
NHTS_PATH = EIA_PATH
DAYV2PUB_PATH = "%s/DAYV2PUB.CSV" %(NHTS_PATH)
HHV2PUB_PATH = "%s/HHV2PUB.CSV" %(NHTS_PATH)
PERV2PUB_PATH = "%s/PERV2PUB.CSV" %(NHTS_PATH)
VEHV2PUB_PATH = "%s/VEHV2PUB.CSV" %(NHTS_PATH)

def query_read(cur, SQL):
    cur.execute(SQL)
    rows = cur.fetchall();
    for row in rows:
      for col in row:
        if( col is not None): # handle column with NULL 
          sys.stdout.write(str(col) + "\t") 
      print

def load_csv_nhts(cur):
  try:
    cur.execute(NHTS.CREATE_HH)
    cur.execute(NHTS.CREATE_DAY)
    cur.execute(NHTS.CREATE_VEH)
    cur.execute(NHTS.CREATE_PER)
  except:
    print "failed creating NHTS tables"
  
  load_csv_nhts_table(HHV2PUB_PATH, cur, NHTS.TABLE_HH)
  load_csv_nhts_table(VEHV2PUB_PATH, cur, NHTS.TABLE_VEH)
  load_csv_nhts_table(PERV2PUB_PATH, cur, NHTS.TABLE_PER)
  load_csv_nhts_table(DAYV2PUB_PATH, cur, NHTS.TABLE_DAY)

def load_csv_nhts_table(path, cur, TABLE):
  with open(path, 'rb') as csvfile:
    rows = csv.reader(csvfile, delimiter = ',', quotechar='\"')
    header = rows.next() 
    header = (','.join(header))
    
    sql_insert = '''INSERT INTO %s(%s) VALUES''' %(TABLE, header)
    
    i = 0
    for row in rows: 
      i+=1
      row = ("%s%s%s" %("\'", col, "\'") for col in row )
      sql_insert += "(" + (','.join( row )) + "),"
      
      if i == 1000: # every 1000 tuples per batch for insertion
        sql_insert = sql_insert[:-1] + ';'
        cur.execute(sql_insert)
        #reset
        i = 0
        sql_insert = '''INSERT INTO %s(%s) VALUES''' %(TABLE, header)

    sql_insert = sql_insert[:-1] + ';'
    cur.execute(sql_insert)
    print "== DONE LOADING DATA FROM %s ==" %(path)

def load_csv_eia(cur):
  try: 
    cur.execute(EIA.CREATE_EIA)
    cur.execute(EIA.CREATE_ELEC)
    cur.execute(EIA.CREATE_TRANS)
    cur.execute(EIA.CREATE_MKWH)
  except:
    print "failed creating EIA tables"

  load_csv_eia_tables(CO2_E_PATH, cur, EIA.TABLE_EIA, EIA.TABLE_ELEC) 
  load_csv_eia_tables(CO2_T_PATH, cur, EIA.TABLE_EIA, EIA.TABLE_TRANS) 
  load_csv_eia_tables(MKWH_PATH, cur, EIA.TABLE_EIA, EIA.TABLE_MKWH) 

def load_csv_eia_tables(path, cur, TABLE_0, TABLE_1):
  csvfile = open(path, 'rb')
  rows = csv.reader(csvfile, delimiter = ',', quotechar='\"')
  next(rows, None)
   
  sql_insert0 = '''INSERT INTO %s VALUES ''' %(TABLE_0)
  sql_insert1 = '''INSERT INTO %s VALUES ''' %(TABLE_1)

  column_order = "1"
  for row in rows:
     if(row[3] == column_order): # insert only distinct tuples
      sql_insert0 += "('%s', %s, '%s', '%s')," %(row[0], row[3], row[4], row[5])
      column_order = str(int(column_order) + 1) 
     
     try: ## if the column Value (row[2]) is a float
      float(row[2]) 
      value = row[2]
     except: ## else, it's "NOT AVAILABLE",  make it a NULL
      value = 'NULL'
     
     sql_insert1 += "('%s', %s, %s)," %(row[0], row[1], value)
       
  sql_insert0 = sql_insert0[:-1]  + ";" # replace ending ',' with ';'
  sql_insert1 = sql_insert1[:-1]  + ";"
  
  cur.execute(sql_insert0)
  cur.execute(sql_insert1)
  print "== DONE LOADING DATA FROM %s ==" %(path)

def main():
  conn = psycopg2.connect(LOCAL_DB)
  cur = conn.cursor()
  ''' 
  cur.execute("DROP TABLE %s,%s,%s,%s, %s,%s,%s,%s;" %(EIA.TABLE_EIA,\
    EIA.TABLE_ELEC, EIA.TABLE_TRANS, EIA.TABLE_MKWH, NHTS.TABLE_PER,\
    NHTS.TABLE_VEH, NHTS.TABLE_DAY, NHTS.TABLE_HH) ) 
  conn.commit()
  '''
  load_csv_eia(cur)
  load_csv_nhts(cur) 

  conn.commit()
  conn.close()

main()
