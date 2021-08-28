# DROP TABLES

crime_fact_table_drop = "DROP TABLE IF EXISTS crime_fact"
address_table_drop = "DROP TABLE IF EXISTS address_dim"
date_time_table_drop = "DROP TABLE IF EXISTS datetime_dim"
offense_table_drop = "DROP TABLE IF EXISTS offense_dim"
police_beat_table_drop = "DROP TABLE IF EXISTS police_beat_dim"
premise_table_drop = "DROP TABLE IF EXISTS premise_dim"

# CREATE TABLES

# FACT TABLE
crime_fact_table_create = ("""
CREATE TABLE crime_fact (
    crime_fact_id int  NOT NULL,
    numOffenses int  NOT NULL,
    temp int  NOT NULL,
    feels_like int  NOT NULL,
    humidity float  NOT NULL,
    rain int  NOT NULL,
    snow int  NOT NULL,
    offense_dim_id int  NOT NULL,
    police_beat_dim_id int  NOT NULL,
    premises_dim_id int  NOT NULL,
    address_dim_id int  NOT NULL,
    datetime_id int  NOT NULL,
    CONSTRAINT crime_fact_pk PRIMARY KEY (crime_fact_id)
);""")

address_table_create = ("""
CREATE TABLE address_dim (
    address_id int  NOT NULL,
    full_address varchar  NOT NULL,
    CONSTRAINT address_pk PRIMARY KEY (address_id)
);
""")

datetime_table_create = ("""
CREATE TABLE datetime_dim (
    datetime_id int  NOT NULL,
	date_time varchar NOT NULL,
    hour int  NOT NULL,
    day int  NOT NULL,
    week int  NOT NULL,
    month int  NOT NULL,
    year int  NOT NULL,
    weekday int  NOT NULL,
    CONSTRAINT datetime_pk PRIMARY KEY (datetime_id)
);""")


offense_table_create = ("""
CREATE TABLE offense_dim (
    offense_id int  NOT NULL,
    offense_type varchar  NOT NULL,
    CONSTRAINT offense_pk PRIMARY KEY (offense_id)
);""")


policebeat_table_create = ("""
CREATE TABLE police_beat_dim (
    police_beat_id int  NOT NULL,
    beat_name varchar(10)  NOT NULL,
    CONSTRAINT police_beat_pk PRIMARY KEY (police_beat_id)
);""")


premise_table_create = ("""
CREATE TABLE premise_dim (
    premise_id int  NOT NULL,
    premise_location varchar  NOT NULL,
    CONSTRAINT premise_pk PRIMARY KEY (premise_id)
);""")


#####################################################
# INSERT INTO RECORDS

#####################################################


crime_fact_table_insert = ("""
INSERT INTO crime_fact (
	crime_fact_id,
    numOffenses, 
    temp, 
    feels_like, 
    humidity, 
    rain, 
    snow,   
    offense_dim_id, 
    police_beat_dim_id,
    premises_dim_id, 
    address_dim_id, 
    datetime_id 
)
VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
-- ON CONFLICT (crime_fact_id) DO NOTHING
""")


address_table_insert = ("""
INSERT INTO address_dim (
	address_id,
	full_address
)
VALUES (%s,%s)

-- ON CONFLICT (address_id) DO NOTHING
""")


datetime_table_insert = ("""
INSERT INTO datetime_dim (
	datetime_id,
	date_time,
    hour,
    day,
    week ,
    month ,
    year,
    weekday
)
VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
-- ON CONFLICT (datetime_id) DO NOTHING
""")


offense_table_insert = ("""
INSERT INTO offense_dim (
	    offense_id,
	    offense_type
)
VALUES (%s,%s)
-- ON CONFLICT (offense_id) DO NOTHING
""")


police_beat_table_insert = ("""
INSERT INTO police_beat_dim (
	police_beat_id,
	beat_name
)
VALUES (%s,%s)
-- ON CONFLICT (police_beat_id) DO NOTHING
""")


premise_table_insert = ("""
INSERT INTO premise_dim (
	premise_id,
	premise_location
)
VALUES (%s,%s)
-- ON CONFLICT (premise_id) DO NOTHING
""")


# QUERY LISTS

create_table_queries = [crime_fact_table_create, address_table_create,
                        datetime_table_create, offense_table_create, policebeat_table_create, premise_table_create]
drop_table_queries = [crime_fact_table_drop, address_table_drop, date_time_table_drop,
                      offense_table_drop, police_beat_table_drop, premise_table_drop]
