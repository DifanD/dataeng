
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
web_scrape_df = glueContext.create_dynamic_frame.from_catalog(database='web_scrape', table_name='webscrapede')
web_scrape_df.printSchema()
web_scrape_df = web_scrape_df.drop_fields(paths='col23')
# Rename
web_scrape_df = web_scrape_df.rename_field('col1','link').rename_field('col2','name').rename_field('col3','rate').rename_field('col4','no_reviews').rename_field('col0','row_id')
web_scrape_df = web_scrape_df.rename_field('col5','room_type').rename_field('col6','host_tags').rename_field('col7','host_join_month').rename_field('col8','host_res')
web_scrape_df = web_scrape_df.rename_field('col9','discount').rename_field('col10','disc_price').rename_field('col11','orig_price').rename_field('col12','no_guest')
web_scrape_df = web_scrape_df.rename_field('col13','no_bedroom').rename_field('col14','no_bed').rename_field('col15','no_bath').rename_field('col16','clean_rate')
web_scrape_df = web_scrape_df.rename_field('col17','acc_rate').rename_field('col18','comm_rate').rename_field('col19','loc_rate').rename_field('col20','check_in_rate')
web_scrape_df = web_scrape_df.rename_field('col21','value_rate').rename_field('col22','property_id').rename_field('col24','host_id').rename_field('col25','neibourgh').rename_field('col26','latitude').rename_field('col27','longitude')

web_scrape_df.printSchema()
# Filter out the original header
web_scrape_df= web_scrape_df.filter(
    f = lambda x:x['row_id'] != '0'
)
# Verify that the original header is successfully removed
web_scrape_df.show()
# Drop auto-generated row_id column
web_scrape_df = web_scrape_df.drop_fields(paths='row_id')
web_scrape_df.printSchema()
webs_df = web_scrape_df.apply_mapping(
    [ 
        ("property_id","string","property_id","int"),
        ("disc_price", "string","disc_price", "float"),
        ("orig_price","string","orig_price","float"),
        ("discount", "string", "discount", "boolean"),
        ("rate","string" ,"rate","float"),
        ("clean_rate", "string", "clean_rate", "float"),
        ("acc_rate","string" ,"acc_rate","float"),
        ("loc_rate","string", "loc_rate","float"),
        ("comm_rate","string", "comm_rate","float"),
        ("value_rate","string", "value_rate","float"),
        ("check_in_rate","string", "check_in_rate","float"),
        ("no_guest", "string", "no_guest", "int"),
        ("no_bedroom","string" ,"no_bedroom","int"),
        ("no_bed","string", "no_bed","int"),
        ('host_id','string','host_id','int'),
        ('latitude','string','latitude','float'),
        ('longitude','string','longitude','float'),
        ("host_tags", "string","host_tags", "string"),
        ("host_join_month", "string", "host_join_month", "string"),
        ("host_res", "string","host_res", "string"),
        ("room_type","string", "room_type","string"),
        ("link", "string", "link", "string"),
        ("name", "string", "name", "string"),
        ("no_reviews","string", "no_reviews","string"),
        ('neibourgh','string','neibourgh','string'),
        
    ]
)
webs_df.printSchema()
# Import another crime_related dataset
df_crime = glueContext.create_dynamic_frame.from_catalog(database='staticdata', table_name='crimeprop')
df_crime= df_crime.drop_fields(paths='col0')
df_crime= df_crime.rename_field('col1','Date').rename_field('col2','Report_code').rename_field('col3','Report_desc').rename_field('col4','Inci_cat').rename_field('col11','cur_sup_dist').rename_field('col12','cur_police_dist')
df_crime = df_crime.rename_field('col5','Inci_subcat').rename_field('col6','police_dis').rename_field('col7','ana_neighbour').rename_field('col8','latitude').rename_field('col9','longitude').rename_field('col10','neighbour')

df_crime.printSchema()
#Split original dataset into smaller dataset to relational tables
host= dyf.apply_mapping(
    [
        ("id","string","id","string"),
        ("host_tags", "string","host_tags", "string"),
        ("host_join_month", "string", "host_join_month", "Date"),
        ("host_res", "string","host_res", "string"),
        ("rate","string" ,"rate","float"),
        ("clean_rate", "string", "clean_rate", "float"),
        ("acc_rate","string" ,"acc_rate","float"),
        ("loc_rate","string", "loc_rate","float"),
        ("comm_rate","string", "comm_rate","float"),
        ("value_rate","string", "value_rate","float"),
        ("check_in_rate","string", "check_in_rate","float"),
        ("no_guest", "string", "no_guest", "int"),
        ("no_bedroom","string" ,"no_bedroom","int"),
        ("no_bed","string", "no_bed","int"),
        ("room_type","string", "room_type","string"),
        
    ]
)

Price = dyf.apply_mapping(
    [
        ("id","string","id","string"),
        ("disc_price", "string","disc_price", "float"),
        ("orig_price","string","orig_price","float"),
        ("discount", "string", "discount", "boolean"),
    ]
)

Rating = dyf.apply_mapping(
    [
        ("id","string","id","string"),
        ("rate","string" ,"rate","float"),
        ("clean_rate", "string", "clean_rate", "float"),
        ("acc_rate","string" ,"acc_rate","float"),
        ("loc_rate","string", "loc_rate","float"),
        ("comm_rate","string", "comm_rate","float"),
        ("value_rate","string", "value_rate","float"),
        ("check_in_rate","string", "check_in_rate","float"),
        
    ]
)

Room = dyf.apply_mapping(
    [
        ("id","string","id","string"),
        ("no_guest", "string", "no_guest", "int"),
        ("no_bedroom","string" ,"no_bedroom","int"),
        ("no_bed","string", "no_bed","int"),
        ("no_bath","string", "no_bath","int"),
        ("room_type","string", "room_type","string"),
    ]
)

Property = dyf.apply_mapping(
    [
        ("id","string","id","string"),
        ("link", "string", "link", "string"),
        ("name", "string", "name", "string"),
        ("no_reviews","string", "no_reviews","string"),
        ("address","string", "address","string"),
    ]
)

Property.printSchema()  
Room.printSchema()  
Rating.printSchema()  
Price.printSchema()  
host.printSchema()
host.printSchema()
# Export back to S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=host,
    connection_type="s3",
    connection_options={"path": "s3://webscrapeagg/host/"},
    format="json",
)

#host_clean.setCatalogInfo(
  #catalogDatabase="webscrapeupdate", catalogTableName="host_clean")
#host_clean.setFormat("glueparquet")
#host_clean.writeFrame(host)
glueContext.write_dynamic_frame.from_options(
    frame=Price,
    connection_type="s3",
    connection_options={"path": "s3://webscrapeagg/Price/"},
    format="json",
)

glueContext.write_dynamic_frame.from_options(
    frame=Room,
    connection_type="s3",
    connection_options={"path": "s3://webscrapeagg/Room/"},
    format="json",
)

glueContext.write_dynamic_frame.from_options(
    frame=Rating,
    connection_type="s3",
    connection_options={"path": "s3://webscrapeagg/Review/"},
    format="json",
)

glueContext.write_dynamic_frame.from_options(
    frame=Property,
    connection_type="s3",
    connection_options={"path": "s3://webscrapeagg/Property/"},
    format="json",
)
# Export back to S3 bucket
crime_clean = glueContext.getSink(
  path="s3://crimeprop",
  connection_type="s3", 
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="crime_clean",
)

crime_clean.setCatalogInfo(
  catalogDatabase="staticdata", catalogTableName="police_csv"
)
crime_clean.setFormat("glueparquet")
crime_clean.writeFrame(df_crime)
job.commit()