# Importing required libraries

from sqlalchemy import create_engine, Column,  Integer, String, Date, event
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects import postgresql

# Postgres database connection string

database = 'postgres+psycopg2://postgres:password@localhost:5432/postgres'

# establishing connection

engine = create_engine(database)

schema_name = "Sumup_Marketing"

Base = declarative_base()

# Schema Design

if not engine.dialect.has_schema(engine, schema_name):
    event.listen(Base.metadata, 'before_create', CreateSchema(schema_name))


class Orders(Base):
    __tablename__ = 'orders'
    __table_args__ = {'schema': schema_name}
    orderid = Column(String, primary_key=True)
    date = Column(Date)
    merchantid = Column(String)
    unit_price = Column(postgresql.MONEY)
    lifetime_value = Column(postgresql.MONEY)
    unit_count = Column(Integer)
    currency = Column(String)
    market = Column(String)
    product_type = Column(String)


class Visits(Base):
    __tablename__ = 'visits'
    __table_args__ = {'schema': schema_name}
    date = Column(postgresql.TIMESTAMP, primary_key=True)
    channelgrouping = Column(String)
    merchantid = Column(String)
    fullvisitorid = Column(String, primary_key=True)
    orderid = Column(Integer)
    goal_fk = Column(Integer)
    market = Column(String)
    schema = schema_name


class AdCosts(Base):
    __tablename__ = 'ad_costs'
    __table_args__ = {'schema': schema_name}
    date = Column(Date, primary_key=True)
    channelgrouping = Column(String, primary_key=True)
    cost = Column(postgresql.MONEY)
    currency = Column(String)
    market = Column(String, primary_key=True)


class ReaderManufacturing(Base):
    __tablename__ = 'reader_manufacturing'
    __table_args__ = {'schema': schema_name}
    date_month = Column(Date, primary_key=True)
    product_type = Column(String, primary_key=True)
    manufacturing_cost = Column(postgresql.MONEY)
    currency = Column(String)


# committing the schema

with engine.connect() as con:
    Base.metadata.create_all(con)

# loading the data to the database

data = ["visits" , "orders", "ad_costs","reader_manufacturing"]

for i in data:

    df = pd.read_csv(i+".csv" , skiprows = 0  , delimiter = "," )
    print(df.dtypes)
    df.to_sql(i, engine, if_exists='append', schema = schema_name, index=False)

# creating views in the database

engine.execute('''CREATE OR REPLACE VIEW "Sumup_Marketing".sales_preformance  AS
                        WITH  
                            T1 as (
                                SELECT  
                                CAST(DATE_TRUNC('quarter', date)  + interval '3 months' - interval '1 day' AS date)  AS Year, market , product_type , SUM(unit_count * unit_price) AS Sales                     
                                FROM
                                "Sumup_Marketing".orders
                                GROUP BY
                                Year, market , product_type)

                        SELECT Year, market , product_type, Sales FROM T1 ''')

engine.execute('''CREATE OR REPLACE VIEW "Sumup_Marketing".acquisition_channel  AS
                        WITH
---- getting orders data and changing date data type to allow for joins 
                            T1 AS(
                                SELECT
                                orderid , date AS order_date , CAST(date_trunc('month' , date ) AS DATE) AS MANF_date , unit_price , unit_count ,  unit_price * unit_count AS Sales , product_type , market
                                FROM
                                "Sumup_Marketing".orders),

--- aggregating data in arrays to avoid duplicates when joining orders with visits

                        T2 AS (
                         SELECT 
                         CAST(orderid AS varchar) AS orderid, ARRAY_AGG(channelgrouping) AS channels , ARRAY_AGG(CAST(date AS date)) AS visit_date
                         FROM
                         "Sumup_Marketing".visits
                         WHERE 
                         orderid IS NOT null
                         GROUP BY 
                         orderid),

--- Joining visits and orders	

                    T3 AS(
                        SELECT 
                        T1.orderid , order_date , unit_price , unit_count , unit_price * unit_count AS Sales , product_type , channels , market , visit_date
                        FROM 
                        T1
                        LEFT JOIN 
                        T2 
                        ON
                        T1.orderid = T2.orderid),

--- getting ads cost data

                    T4 as(
                        SELECT 
                        date AS ad_date , channelgrouping , cost , currency , market 
                        FROM 
                        "Sumup_Marketing".ad_costs),

--- orders ads costs join 

                    t5 AS(
                        SELECT
                        orderid, t3.market , channelgrouping, ad_date , cost , currency 
                        FROM
                        ( 	
                            SELECT
                            orderid , UNNEST(channels) AS channels , UNNEST(visit_date) AS visit_date , market
                            FROM 
                            T3
                        )T3
                      INNER JOIN
                    T4
                    ON 
                    T3.channels = T4.channelgrouping AND T3.visit_date = T4.ad_date AND T3.market = T4.market
                    ORDER BY
                    orderid),

--- orders per ad

                    T6 as(
                        SELECT 
                        COUNT(orderid) AS number_of_orders,  market , channelgrouping, ad_date
                        FROM 
                        T5
                        GROUP BY
                        market , channelgrouping, ad_date),

---  AVG ad cost per order 

                    T7 AS( 
                        SELECT 
                        orderid, T5.market , T5.channelgrouping, T5.ad_date , cost , currency, number_of_orders , cost / number_of_orders AS AVG_cost
                        FROM 
                        T5
                        LEFT JOIN
                        T6
                        ON
                        T5.ad_date = T6.ad_date AND T5.market = T6.market AND T5.channelgrouping = T6.channelgrouping 
                        ORDER BY 
                        orderid),

--- inserting the data into arrays again 

                        T8 AS (
                        SELECT 
                        orderid,  ARRAY_AGG(market) AS market , ARRAY_AGG(channelgrouping) AS channelgrouping , ARRAY_AGG(ad_date) AS ad_date , SUM(number_of_orders) AS number_of_orders, 
                        ARRAY_AGG(cost) AS cost , SUM(AVG_cost) AS AVG_ad_cost , SUM(cost) AS total_ad_cost
                        FROM
                        T7
                        GROUP BY
                        orderid
                        ORDER BY
                        orderid),

---- getting Manufacturing data 
                      T9 AS(
                        SELECT
                        date_month , product_type , manufacturing_cost
                        FROM
                        "Sumup_Marketing".reader_manufacturing)


---Joining all the data 

SELECT
T1.orderid , order_date , MANF_date , unit_price , unit_count , Sales , T1.product_type , T1.market , channelgrouping[array_upper(channelgrouping , 1)] as last_channel , channelgrouping[1] as first_channel , ad_date ,  number_of_orders,
AVG_ad_cost , total_ad_cost , manufacturing_cost , CASE WHEN AVG_ad_cost IS NULL THEN (SALES - manufacturing_cost) ELSE (Sales - AVG_ad_cost - manufacturing_cost ) END  AS Profit
FROM
T1
LEFT JOIN
T8 
ON
T1.orderid = T8.orderid
LEFT JOIN
T9
ON 
MANF_date = date_month and T1.product_type = T9.product_type
ORDER BY
orderid
     ''')

engine.execute('''CREATE OR REPLACE VIEW "Sumup_Marketing".profitable_acquisition_channel  AS
                        WITH
---- getting orders data and changing date data type to allow for joins 
                            T1 AS(
                                SELECT
                                orderid , date AS order_date , CAST(date_trunc('month' , date ) AS DATE) AS MANF_date , unit_price , unit_count ,  unit_price * unit_count AS Sales , product_type , market
                                FROM
                                "Sumup_Marketing".orders),

--- aggregating data in arrays to avoid duplicates when joining orders with visits

                            T2 AS(
                                 SELECT 
                                 CAST(orderid AS varchar) AS orderid, ARRAY_AGG(channelgrouping) AS channels , ARRAY_AGG(CAST(date AS date)) AS visit_date
                                 FROM
                                "Sumup_Marketing".visits
                                 WHERE 
                                 orderid IS NOT null
                                 GROUP BY 
                                 orderid),

--- Joining visits and orders

                        T3 AS(
                            SELECT 
                            T1.orderid , order_date , unit_price , unit_count , unit_price * unit_count AS Sales , product_type , channels , market , visit_date
                            FROM 
                            T1
                            LEFT JOIN 
                            T2 
                            ON
                            T1.orderid = T2.orderid),

--- getting ads cost data

                        T4 as(
                            SELECT 
                            date AS ad_date , channelgrouping , cost , currency , market 
                            FROM 
                            "Sumup_Marketing".ad_costs),

--- orders ads costs join 

                        t5 AS(
                        SELECT
                        orderid, t3.market , channelgrouping, ad_date , cost , currency 
                        from
                            ( 
                                SELECT
                                orderid , UNNEST(channels) AS channels , UNNEST(visit_date) AS visit_date , market
                                FROM 
                                T3
                            )T3
                        INNER JOIN
                        T4
                        ON 
                        T3.channels = T4.channelgrouping AND T3.visit_date = T4.ad_date AND T3.market = T4.market
                        ORDER BY
                        orderid),

--- orders per ad

                        T6 as(
                            SELECT 
                            COUNT(orderid) AS number_of_orders,  market , channelgrouping, ad_date
                            FROM 
                            T5
                            GROUP BY
                            market , channelgrouping, ad_date),

---  AVG ad cost per order 

                        T7 AS( 
                            SELECT 
                            orderid, T5.market , T5.channelgrouping, T5.ad_date , cost , currency, number_of_orders , cost / number_of_orders AS AVG_cost
                            FROM 
                            T5
                            LEFT JOIN
                            T6
                            ON
                            T5.ad_date = T6.ad_date AND T5.market = T6.market AND T5.channelgrouping = T6.channelgrouping 
                            ORDER BY 
                            orderid),

--- inserting the data into arrays again 

                            T8 AS (
                                SELECT 
                                orderid,  ARRAY_AGG(market) AS market , ARRAY_AGG(channelgrouping) AS channelgrouping , ARRAY_AGG(ad_date) AS ad_date , SUM(number_of_orders) AS number_of_orders, 
                                ARRAY_AGG(cost) AS cost , SUM(AVG_cost) AS AVG_ad_cost , SUM(cost) AS total_ad_cost
                                FROM
                                T7
                                GROUP BY
                                orderid
                                ORDER BY
                                orderid),

---- getting Manufacturing data 

                          T9 AS(	
                            SELECT
                            date_month , product_type , manufacturing_cost
                            FROM
                            "Sumup_Marketing".reader_manufacturing)


---Joining all the data 

            SELECT
            T1.orderid , order_date , MANF_date , unit_price , unit_count , Sales , T1.product_type , T1.market , channelgrouping , ad_date ,  number_of_orders,
            AVG_ad_cost , total_ad_cost , manufacturing_cost , CASE WHEN AVG_ad_cost IS NULL THEN (SALES - manufacturing_cost) ELSE (Sales - AVG_ad_cost - manufacturing_cost ) END  AS Profit
            FROM
            T1
            LEFT JOIN
            T8 
            ON
            T1.orderid = T8.orderid
            LEFT JOIN
            T9
            ON 
            MANF_date = date_month and T1.product_type = T9.product_type
            where cast(CASE WHEN AVG_ad_cost IS NULL THEN (SALES - manufacturing_cost) ELSE (Sales - AVG_ad_cost - manufacturing_cost ) END as numeric) > 0
            ORDER BY
            orderid
     ''')



print ("Completed !")