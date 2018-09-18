
df_business = sqlContext.read.json("s3a://insightdatascience/Yelp/yelp_academic_dataset_business.json")
df_business_vegas = df_business.filter(df_business.city == "Las Vegas").select("business_id", "name", "address", "city", "postal_code", "latitude", "longitude", "state", "stars", "review_count") #28865 business in total
df_review = sqlContext.read.json("s3a://insightdatascience/Yelp/yelp_academic_dataset_review.json").select("review_id", "user_id", "business_id", "stars", "text") #5,996,996 in total records
df_sanitory = sqlContext.read.csv("s3a://insightdatascience/Las_Vegas_Restaurant_Inspections.csv", header=True) #21762 unique, 162977 in total restaurant check

df_restaurant = df_business_vegas.join(df_sanitory, (df_business_vegas.address == df_sanitory.Address) & (df_business_vegas.name == df_sanitory.Restaurant_Name), 'inner') #2564 joined result
df = df_restaurant.join(df_review, "business_id", 'inner')
df.cache()
df.groupby('user_id').agg({'business_id':'count'}).withColumnRenamed("count(business_id)", "restaurant_count")
df.groupby('business_id').agg({'user_id':'count'}).show()



