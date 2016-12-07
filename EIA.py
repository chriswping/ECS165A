TABLE_EIA = "EIA"
TABLE_ELEC = "EIA_ELEC"
TABLE_TRANS = "EIA_TRANS"
TABLE_MKWH = "EIA_MKWH"

CREATE_EIA  = '''CREATE TABLE %s(
                  MSN           VARCHAR(10),
                  Column_Order  INT,
                  Description   VARCHAR(128),
                  Unit          VARCHAR(64)
                 );''' %(TABLE_EIA)

CREATE_ELEC = '''CREATE TABLE %s(
                  MSN       VARCHAR(10),
                  YYYYMM    INT,
                  Value     DECIMAL(12,3)
                  );
                  ''' %(TABLE_ELEC)

CREATE_TRANS = '''CREATE TABLE %s(
                   MSN     VARCHAR(10),
                   YYYYMM    INT,
                   Value     DECIMAL(12,3)
                   );
                   ''' %(TABLE_TRANS)

CREATE_MKWH = '''CREATE TABLE %s(
                  MSN   VARCHAR(10),
                  YYYYMM    INT,
                  Value     DECIMAL(12,3)
                  );
                  ''' %(TABLE_MKWH)

