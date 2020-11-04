'''
------------------------
SI 618 Project 1 - Data Computation
by Yin Kwong John Lee
------------------------
This script aims produce data for answering the three questions (stated below). Query approaches in Spark and SparkSQL are used to extract meaning out of the company metrics.

To run on Fladoop cluster:
spark-submit --master yarn --num-executors 16 --driver-memory 8g --executor-memory 8g --executor-cores 2 data_computation.py

To get results:
    
Question1:
    hadoop fs -getmerge viable_loc_year_prop viable_loc_year_prop.tsv
    hadoop fs -getmerge viable_prop_year_loc viable_prop_year_loc.tsv
    hadoop fs -getmerge viable_year_prop_loc viable_year_prop_loc.tsv
    
Question2:
    hadoop fs -getmerge viable_loc_year_prop viable_loc_year_prop.tsv
    hadoop fs -getmerge viable_prop_year_loc viable_prop_year_loc.tsv
    hadoop fs -getmerge viable_year_prop_loc viable_year_prop_loc.tsv
    hadoop fs -getmerge state_avg_viability state_avg_viability.csv
    hadoop fs -getmerge corporate_viability corporate_viability.csv
    
Question3:
    ## Use corporate_viability.csv for visualization

'''

# Importing libraries
from pyspark import SparkContext,Row
from pyspark import SQLContext

## Upgrade system memory
SparkContext.setSystemProperty('spark.executor.memory', '20g')
# Defining spark context
sc = SparkContext(appName="PySparksi618f20project1")
sqlContext=SQLContext(sc)


#%%
# Q1: How do viable investment opportunities change at different locations throughout the years?
    
import json
input_file = sc.textFile("company_metrics.json")

# Function to count the number of viable stocks by year 
def viable_by_loc_year(data):
    result_list=[]
    ## get location of companies
    location=data.get('location',None)
    ## get year of filings 
    year=data.get('fiscal_year',None)
    ## filter to contain viable investments 
    if data.get('ROE',None)>0.12 and data.get('ROTC',None)>0.12 and data.get('NPM',None)>0.12 and data.get('CURRENT_RATIO',None)>1 and data.get('DEBT_EBIT',None)<3:
        viable_stocks=1 ## binary: if this is a viable stock 
        unviable_stocks=0
    else:
        unviable_stocks=1
        viable_stocks=0 ## binary: if this is not a viable stock 
    
    result_list.append(((location, year),(viable_stocks, unviable_stocks)))
    return result_list

# Reduce to include the location, year, and proportion of viable investments
## -----------------------Sort by location-> year-> proportion----------------
loc_year_prop=input_file.map(lambda line: json.loads(line))\
                 .flatMap(viable_by_loc_year)\
                 .reduceByKey(lambda x,y: (float(x[0]+y[0]),float(x[1]+y[1])))\
                 .map(lambda z: (z[0][0],z[0][1],z[1][0]/(z[1][0]+z[1][1])))\
                 .sortBy(lambda s: (s[0],-s[1],-s[2]))
                     
# Write to tsv file 
loc_year_prop.map(lambda rec: rec[0].encode('utf-8', 'ignore').decode('utf-8')+ "\t" +str(rec[1])+ "\t" +str(rec[2])).saveAsTextFile("viable_loc_year_prop")

# --------------------------Sort by proportion-> year-> location-------------------
prop_year_loc=input_file.map(lambda line: json.loads(line))\
                 .flatMap(viable_by_loc_year)\
                 .reduceByKey(lambda x,y: (float(x[0]+y[0]),float(x[1]+y[1])))\
                 .map(lambda z: (z[0][0],z[0][1],z[1][0]/(z[1][0]+z[1][1])))\
                 .sortBy(lambda s: (-s[2],-s[1],s[0]))

# Write to tsv file 
prop_year_loc.map(lambda rec: rec[0].encode('utf-8', 'ignore').decode('utf-8')+ "\t" +str(rec[1])+ "\t" +str(rec[2])).saveAsTextFile("viable_prop_year_loc")

# -------------------Sort by year-> prop-> location----------------------------
year_prop_loc=input_file.map(lambda line: json.loads(line))\
                 .flatMap(viable_by_loc_year)\
                 .reduceByKey(lambda x,y: (float(x[0]+y[0]),float(x[1]+y[1])))\
                 .map(lambda z: (z[0][0],z[0][1],z[1][0]/(z[1][0]+z[1][1])))\
                 .sortBy(lambda s: (-s[1],-s[2],s[0]))

# Write to tsv file 
year_prop_loc.map(lambda rec: rec[0].encode('utf-8', 'ignore').decode('utf-8')+ "\t" +str(rec[1])+ "\t" +str(rec[2])).saveAsTextFile("viable_year_prop_loc")

#%%

# Q2: How are investment opportunities compared on a state and corporate level?

# Adjust data types of rdd objects
loc_year_prop=loc_year_prop.map(lambda p: Row(location=p[0].encode('utf-8', 'ignore').decode('utf-8'), fiscal_year=int(p[1]), viable_prop=float(p[2])))
# Convert rdd data objects to dataframe. These are the filtered companies that meet our slection criteria
viable_stocks= sqlContext.createDataFrame(loc_year_prop)
# Register temporary table
viable_stocks.registerTempTable("viable_stocks")

# Loading company_metrics.json. Only selecting metrics columns
stocks_df=sqlContext.read.json("company_metrics.json").select("ticker", "location", "fiscal_year","ROE", "ROTC", "NPM", "CURRENT_RATIO", "DEBT_EBIT", "DIV_PAYOUT_RATIO", "EPS", "REPS")
# Register data as table 
stocks_df.registerTempTable('stocks_df')

# Join viable_stocks with stocks_df to append the proportion of viable businesses for each company's state in a particular year 
## Note that the viable stocks did not filter out the unviable businesses. Rather it aggregates their counts. So, we still have to filter out the unviable businesses using SQL queries

# ---------------------------------From a State Level----------------------------
state_avgs_df=sqlContext.sql('select stocks_df.location, mean(viable_prop) as state_avg_viablity, mean(ROE) as avg_roe, mean(ROTC) as avg_rotc, mean(NPM) as avg_npm, mean(CURRENT_RATIO) as avg_current_ratio, mean(DEBT_EBIT) as avg_debt_ebit, mean(DIV_PAYOUT_RATIO) as avg_div_payout_ratio, mean(EPS/REPS) as avg_eps_reps from stocks_df join viable_stocks on stocks_df.location=viable_stocks.location and stocks_df.fiscal_year=viable_stocks.fiscal_year group by stocks_df.location order by state_avg_viablity desc')

# Save to csv
state_avgs_df.write.option("header","false").csv("state_avg_viability")

# ---------------------------------From a Corporate level------------------------
# Just like the reduction job above but using SQL query rather than rdd

corporate_avgs_df=sqlContext.sql('select ticker, location, sum(case when ROE>0.12 and ROTC>0.12 and  NPM>0.12 and CURRENT_RATIO>1 and DEBT_EBIT<3 and DIV_PAYOUT_RATIO>0.35 and DIV_PAYOUT_RATIO<0.55 then 1 else 0 end)/(count(*) * 1.0) as corporate_viability, mean(ROE) as avg_roe, mean(ROTC) as avg_rotc, mean(NPM) as avg_npm, mean(CURRENT_RATIO) as avg_current_ratio, mean(DEBT_EBIT) as avg_debt_ebit, mean(DIV_PAYOUT_RATIO) as avg_div_payout_ratio, mean(EPS/REPS) as avg_eps_reps from stocks_df group by ticker, location order by corporate_viability desc')

# Save to csv
corporate_avgs_df.write.option("header","false").csv("corporate_viability")

##%%

#Q3: Are there any good indicators for identifying viable long-term investments? 

## Corporate_avgs_df exported for visualization
## Saved to csv from the last part for visualization

