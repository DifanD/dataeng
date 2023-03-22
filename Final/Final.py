
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
airbnb_static = glueContext.create_dynamic_frame.from_catalog(database='arn:aws:927850244355:eu-west-2::staticdata/airbnbstatic', table_name='air_ref_parquet1_gzip')
airbnb_static.printSchema()
crime_raw = glueContext.create_dynamic_frame.from_catalog(database='staticdata', table_name='crime_raw_parquet2_gzip')
crime_raw.printSchema()
crime_final = crime_raw.apply_mapping(
    [
        ('Incident_Date','Date','Incident_Date','Date'),
        ('Incident_Category','string','Incident_Category','string'),
        ('Incident_Subcategory','string','Incident_Subcategory','string'),
        ('latitude','double','latitude','float'),
        ('longitude','double','longitude','float'),
        ('Neighborhoods','double','neighbour','string'),
    ]
)

crime_output = crime_final.repartition(1)
crime_final_output = glueContext.write_dynamic_frame.from_options(frame = crime_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/crime/"}, 
                                                              format = "parquet", transformation_ctx = "crime_final_output")
web_scrape_raw = glueContext.create_dynamic_frame.from_catalog(database='webscrape new', table_name='webscrapede')
web_scrape_raw.printSchema()
# Merge dataset (air_ref and new web_scrape_raw)
web_scrape_final = web_scrape_raw.join(
    paths1=["id"], paths2=["id"], frame2=airbnb_static
) 
# Host_identity
host_identification = web_scrape_final.apply_mapping(
    [
        ('id','long','id','int'),
        ('host_id','long','host_id','int'),
        ('hosted_by','string','hosted_by','string'),
        
    ]
)

host_id = host_identification.repartition(1)
host_id_output = glueContext.write_dynamic_frame.from_options(frame = host_id, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/hostiden/"}, 
                                                              format = "parquet", transformation_ctx = "host_id_output")
        
# Property_features

Property_features= web_scrape_final.apply_mapping(
    [
        ('id','long','id','int'),
        ('room_type','string',),
        ('Studio','double','Studio','boolean'),
        ('bedroom','double','bedroom','int'),
        ('beds','double','beds','float'),
        ('bathroom','double','bathroom','float'),
        ('shared_bathrooms','double','shared_bathrooms','float'),
        ('private_bathroom','double','private_bathroom','float'),
        ('neighbourhood_cleansed','string','neighbour','string'),
        ('guests', 'double','guests', 'int'),
    ]
)

prop_feature_output = Property_features.repartition(1)
prop_f_output = glueContext.write_dynamic_frame.from_options(frame = prop_feature_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/property_feature/"}, 
                                                              format = "parquet", transformation_ctx = "prop_f_output")
        


        
# Host basic 
host_basic =  web_scrape_final.apply_mapping(
    [
        ('host_id','long','host_id','int'),
        ('host_join','string','host_join','Date'),
        ('host_url','string','host_url','string'),
        ('host_name','string','host_name','string'),
        ('host_about','string','host_about','string'),
    ]
)

host_basic_output = host_basic.repartition(1)
host_b_output = glueContext.write_dynamic_frame.from_options(frame = host_basic_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/host_basic/"}, 
                                                              format = "parquet", transformation_ctx = "host_b_output")

host_activity = web_scrape_final.apply_mapping(
    [
        ('host_id','long','host_id','int'),
        ('host_tags','array','host_tags','array'),
        ('host_listings_count', 'long','host_listings_count', 'int'),
        ('host_acceptance_rate', 'string','host_acceptance_rate','float'),
        ('host_resp', 'array','host_resp', 'array'),
        ('reviews_per_month','double','reviews_per_month','int'),
    ]
)

host_activity_relationalized = host_activity.relationalize(
    "l_root", "s3://readytouseweb/host_activity/"
)
        
property_booking = web_scrape_final.apply_mapping(
    [
        ('id','long','id','int'),
        ('check_out_date','date','check_out_date','date'),
        ('check_in_date','date','check_in_date','date'),
        ('discount', 'boolean','discount', 'boolean'),
        ('orig_price','long','orig_price','float'),
        ('disc_price','long','disc_price','float'),
    ]
)
prop_booking_output = property_booking.repartition(1)
prop_book_output = glueContext.write_dynamic_frame.from_options(frame = prop_booking_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/property_booking/"}, 
                                                              format = "parquet", transformation_ctx = "prop_book_output")
property_review = web_scrape_final.apply_mapping(
    [
        ('id','long','id','int'),
        ('rating','string','rating_score','float'),
        ('Value','string','Value_score','float'),
        ('no_reviews','string','no_reviews','string'),
        ('Accuracy','string','Accuracy_score','float'),
        ('Communication','string','Communication_score','float'),
        ('Cleanliness','string','Cleanliness_score','float'),
        ('Location','string','Location_score','string'),
        ('Check-in', 'string','Check_in_score','float'),
    ]
)

prop_review_output = property_review.repartition(1)
prop_r_output = glueContext.write_dynamic_frame.from_options(frame = prop_review_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/property_booking_review/"}, 
                                                              format = "parquet", transformation_ctx = "prop_r_output")
prop_info = web_scrape_final.apply_mapping(
    [
        ('id','long','id','int'),
        ('latitude','double','latitude','float'),
        ('longitude','double','longitude','float'),
        ('name','string','name','string'),
        ('web_link','string','web_link','string'),
    ]
)

prop_info_output = prop_info.repartition(1)
prop_id_output = glueContext.write_dynamic_frame.from_options(frame = prop_info_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/property_id/"}, 
                                                              format = "parquet", transformation_ctx = "prop_id_output")
crime = web_scrape_final.apply_mapping(
    [
prop_feature_output = Property_features.repartition(1)
prop_f_output = glueContext.write_dynamic_frame.from_options(frame = prop_feature_output, 
                                                              connection_type = "s3", 
                                                              connection_options = {"path": "s3://readytouseweb/property_feature/"}, 
                                                              format = "parquet", transformation_ctx = "prop_f_output")
        
web_scrape_final.printSchema()
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()