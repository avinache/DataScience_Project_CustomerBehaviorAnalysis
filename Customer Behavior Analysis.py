# -*- coding: utf-8 -*-

import mysql.connector as db
import pandas as pd


#Create MySQL connection
user ='CB_Analysis'
password='CBAnalysis123'
host='localhost'
db_name='cba'
db_Connection=db.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

curr=db_Connection.cursor()

#SQL query to create customer Table in DataBase
sql="""CREATE TABLE Customer 
(
CustomerID INT,
CustomerName varChar(100),
Email varChar(100),
Gender varChar(10),
Age INT,
GeographyID INT
);"""
curr.execute(sql)
#Read data from the given customers.csv file and store it as DataFrame
customer_DF=pd.read_csv("customers.csv")

#insert query
insert="""INSERT INTO customer
(CustomerID, CustomerName, Email, Gender, Age, GeographyID)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=customer_DF.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create customer_reviews Table in DataBase
sql="""CREATE TABLE customer_reviews 
(
ReviewID INT,
CustomerID INT,
ProductID INT,
ReviewDate DATE,
Rating FLOAT,
ReviewText varchar(100)
);"""
curr.execute(sql)
#Read data from the given customer_reviews.csv file and store it as DataFrame
customer_reviews_DF=pd.read_csv("customer_reviews.csv")

#insert query
insert="""INSERT INTO customer_reviews
(ReviewID, CustomerID, ProductID, ReviewDate, Rating, ReviewText)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=customer_reviews_DF.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create customer_journey Table in DataBase
sql="""CREATE TABLE customer_journey 
(
JourneyID INT,
CustomerID INT,
ProductID INT,
VisitDate DATE,
Stage varchar(25),
Action varchar(50),
Duration INT
);"""
curr.execute(sql)
#Read data from the given customer_journey.csv file and store it as DataFrame
customer_journey_DF=pd.read_csv("customer_journey.csv")

#insert query
insert="""INSERT INTO customer_journey
(JourneyID, CustomerID, ProductID, VisitDate, Stage, Action, Duration)
 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=customer_journey_DF.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create engagement_data Table in DataBase
sql="""CREATE TABLE engagement_data 
(
EngagementID INT,
ContentID INT,
ContentType varchar(50),
Likes INT,
EngagementDate DATE,
CampaignID INT,
ProductID INT,
ViewsClicksCombined varchar(50)
);"""
curr.execute(sql)
#Read data from the given engagement_data.csv file and store it as DataFrame
engagement_data_DF=pd.read_csv("engagement_data.csv")

#insert query
insert="""INSERT INTO engagement_data
(EngagementID, ContentID, ContentType, Likes, EngagementDate, CampaignID, ProductID, ViewsClicksCombined)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=engagement_data_DF.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#Add New columns for Views and Clicks in engagement_data Table
sql="""ALTER TABLE engagement_data
ADD COLUMN Views INT,
ADD COLUMN Clicks INT;"""
curr.execute(sql)

#Update values in Views and clicks column by using SUBSTRING_INDEX Function
#SUBSTRING_INDEX(string, delimiter, count), 1:left of delimiter, -1:right of delimiter
update="""UPDATE engagement_data
SET
Views=SUBSTRING_INDEX(ViewsClicksCombined,'-',1),
Clicks=SUBSTRING_INDEX(ViewsClicksCombined,'-',-1);"""
curr.execute(update)
db_Connection.commit()

#SQL query to create geography Table in DataBase
sql="""CREATE TABLE geography 
(
GeographyID INT,
Country varchar(50),
City varchar(50)
);"""
curr.execute(sql)
#Read data from the given geography.csv file and store it as DataFrame
geography_DF=pd.read_csv("geography.csv")

#insert query
insert="""INSERT INTO geography
(GeographyID, Country, City)
 VALUES (%s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=geography_DF.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create products Table in DataBase
sql="""CREATE TABLE products 
(
ProductID INT,
ProductName varchar(75),
Category varchar(50),
Price FLOAT
);"""
curr.execute(sql)
#Read data from the given products.csv file and store it as DataFrame
products_DF=pd.read_csv("products.csv")

#insert query
insert="""INSERT INTO products
(ProductID, ProductName, Category, Price)
 VALUES (%s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=products_DF.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#Since contentType column values are not in same format so change it to unique format (all values in lower case)
sql="""UPDATE engagement_data
SET ContentType = LOWER(ContentType);
"""
curr.execute(sql)
db_Connection.commit()

#Since Stage column values are not in same format so change it to unique format (all values in lower case)
sql="""UPDATE customer_journey
SET Stage = LOWER(Stage);
"""
curr.execute(sql)
db_Connection.commit()

#Since Duration column values contains Null. So replace Null values using COALESCE() function
sql="""UPDATE customer_journey
SET Duration = COALESCE(Duration, 0);
"""
curr.execute(sql)
db_Connection.commit()

#Join customer and engagement 
#Identify max likes, views and clicks based on contentType-Display highest value first
sql="""SELECT 
    e.ContentType,
    SUM(e.Likes) AS Likes,
    SUM(e.Views) AS Views,
    SUM(e.Clicks) AS Clicks
FROM 
    customer c
INNER JOIN 
    engagement_data e
ON 
    c.CustomerID = e.EngagementID
GROUP BY 
    e.ContentType
ORDER BY 
    Likes DESC, Views DESC, Clicks DESC;"""
curr.execute(sql)
ContentData = curr.fetchall()
ContentDataTable = pd.DataFrame(ContentData, columns = curr.column_names)

# Count how many customers are in each stage and action get unique customer count based on stages
sql="""SELECT 
    Stage,
    COUNT(DISTINCT CustomerID) AS CustomerCount
FROM 
    customer_journey
GROUP BY 
    Stage;"""
curr.execute(sql)
Stage_counts = curr.fetchall()
Stage_countsTable = pd.DataFrame(Stage_counts, columns = curr.column_names)
Stage_countsTable

# To calculate the drop-off rate, we will track how many customers enter each stage and how many drop off.
Stage_countsTable['DropOffRate'] = Stage_countsTable['CustomerCount'] / Stage_countsTable['CustomerCount'].sum()
Stage_countsTable['DropOffRate'] = Stage_countsTable['DropOffRate']*100
Stage_countsTable

# Count how many customers are in each stage and action get unique customer count based on stages
sql="""SELECT 
    Action,
    COUNT(DISTINCT CustomerID) AS CustomerCount
FROM 
    customer_journey
GROUP BY 
    Action;"""
curr.execute(sql)
Stage_counts = curr.fetchall()
Stage_countsTable = pd.DataFrame(Stage_counts, columns = curr.column_names)
Stage_countsTable

# To calculate the drop-off rate, we will track how many customers enter each stage and how many drop off.
Stage_countsTable['DropOffRate'] = Stage_countsTable['CustomerCount'] / Stage_countsTable['CustomerCount'].sum()
Stage_countsTable['DropOffRate'] = Stage_countsTable['DropOffRate']*100
Stage_countsTable

# Count how many customers are in each stage & Action - get customer count based on stages & Action
sql="""SELECT 
    Stage,
    Action,
    COUNT(DISTINCT CustomerID) AS CustomerCount
FROM 
    customer_journey
GROUP BY 
    Stage, Action;"""
curr.execute(sql)
StageAction_counts = curr.fetchall()
StageAction_countsTable = pd.DataFrame(StageAction_counts, columns = curr.column_names)
StageAction_countsTable
#filter only checkout stage to find out the drop off rate
checkoutDropPercent = StageAction_countsTable[StageAction_countsTable['Stage']== 'checkout'].sort_values(by='CustomerCount', ascending=False)
#Add dropoffRate column and update find out the percentage of drop off on each action
checkoutDropPercent['DropOffRate'] = checkoutDropPercent['CustomerCount'] / checkoutDropPercent['CustomerCount'].sum()
checkoutDropPercent['DropOffRate'] = checkoutDropPercent['DropOffRate']*100
checkoutDropPercent

#Identify max likes, views and clicks based on CampaignID-Display highest value first
sql="""SELECT 
    CampaignID,
    SUM(Likes) AS Likes,
    SUM(Views) AS Views,
    SUM(Clicks) AS Clicks
FROM 
    engagement_data
GROUP BY 
    CampaignID
ORDER BY 
    Likes DESC, Views DESC, Clicks DESC;"""
curr.execute(sql)
CampaignData = curr.fetchall()
CampaignDataTable = pd.DataFrame(CampaignData, columns = curr.column_names)
CampaignDataTable.head()
CampaignDataTable.tail()

# find the total duration at each stage and action
sql="""SELECT 
    Stage,
    Action,
    SUM(Duration) AS TotalDuration
FROM 
    customer_journey
GROUP BY 
    Stage, Action
ORDER BY
	Stage;"""
curr.execute(sql)
DurationData = curr.fetchall()
DurationDataTable = pd.DataFrame(DurationData, columns = curr.column_names)
DurationDataTable

# Merge customer reviews with product details
# Calculate the average rating for each product in desending order
# Merge the average ratings with the product data using left join
#Replace NaN values with.0.00
sql="""WITH average_ratings AS (
    SELECT 
        ProductID,
        AVG(Rating) AS AverageRating
    FROM 
        customer_reviews
    GROUP BY 
        ProductID
    ORDER BY 
        AverageRating DESC
)

-- Perform the left join with the products table
SELECT 
    p.ProductID,
    p.ProductName,
    p.Price,
    ar.AverageRating
FROM 
    products p
LEFT JOIN 
    average_ratings ar
ON 
    p.ProductID = ar.ProductID
ORDER BY 
    ar.AverageRating DESC, p.Price DESC;"""
curr.execute(sql)
ProductRating = curr.fetchall()
ProductRatingTable = pd.DataFrame(ProductRating, columns = curr.column_names)
ProductRatingTable = ProductRatingTable.fillna(0.00)
ProductRatingTable.tail()

# Merge customer reviews with product details
# Add feedback column based on rating  using 'case' in select query.
sql="""SELECT 
    cr.ProductID,
    cr.CustomerID,
    cr.Rating,
    p.ProductName,
    p.Category,
    p.Price,
    CASE
        WHEN cr.Rating >= 4 THEN 'Positive'
        WHEN cr.Rating = 3 THEN 'Neutral'
        WHEN cr.Rating >= 2 AND cr.Rating < 3 THEN 'Poor'
        ELSE 'Negative'
    END AS Feedback
FROM 
    customer_reviews cr
INNER JOIN 
    products p
ON 
    cr.ProductID = p.ProductID;"""
curr.execute(sql)
FeedBack = curr.fetchall()
FeedBackTable = pd.DataFrame(FeedBack, columns = curr.column_names)
FeedBackTable

# CusReview_Product_DF.pivot_table(index='ProductID', columns='Feedback', values='ProductID', aggfunc='count').fillna(0) value error
#.size() counts the occurrences of each combination of values in df
# Group by 'ProductID' and 'Feedback' and count the occurrences of each value
# unstack() reshapes the result of groupby() (or any hierarchical index) by pivoting one of the index levels into columns
# fill_value=0 replaces any missing values in the reshaped DataFrame with 0.
# kind of pivot table
Feedback_DF = FeedBackTable.groupby(['ProductID','Feedback']).size().unstack(fill_value=0).sort_values(by='ProductID', ascending=True).reset_index()
Feedback_DF

# Calculate the total feedback count for each ProductID
Feedback_DF['TotalFeedbackCount'] = Feedback_DF.drop(columns='ProductID').sum(axis=1)
Feedback_DF.sort_values(by='TotalFeedbackCount', ascending=False)

# Find average rating for each product along with total likes, views, and clicks.
sql="""SELECT 
    r.ProductID,
    AVG(r.Rating) AS AverageRating,
    SUM(e.Likes) AS TotalLikes,
    SUM(e.Views) AS TotalViews,
    SUM(e.Clicks) AS TotalClicks
FROM 
    customer_reviews r
JOIN 
    engagement_data e ON r.ProductID = e.ProductID
GROUP BY 
    r.ProductID
ORDER BY 
    TotalLikes DESC, TotalViews DESC, TotalClicks DESC;"""
curr.execute(sql)
ProductAVG_Rating = curr.fetchall()
ProductAVG_RatingDF = pd.DataFrame(ProductAVG_Rating, columns = curr.column_names)
ProductAVG_RatingDF

# Merge customer and geography tables based on GeographyID
# Merge above result with customer reviews based on customerID
#Find the average rating from customer review table
#Final merge the average rating with output result of previous merged result 
sql="""WITH average_ratings AS (
    SELECT 
        ProductID,
        AVG(Rating) AS AverageRating
    FROM 
        customer_reviews
    GROUP BY 
        ProductID
    ORDER BY 
        AverageRating DESC
)
SELECT DISTINCT crg.*, ar.AverageRating
FROM 
(SELECT DISTINCT cr.*, cg.CustomerName, cg.Email, cg.Gender, cg.Age, cg.GeographyID, cg.Country, cg.City

    FROM customer_reviews cr
    INNER JOIN (
        SELECT DISTINCT c.*, g.Country, g.City
        FROM customer c
        INNER JOIN geography g
            ON c.GeographyID = g.GeographyID
    ) cg
        ON cr.CustomerID = cg.CustomerID) crg
LEFT JOIN 
    average_ratings ar
ON 
    crg.ProductID = ar.ProductID
ORDER BY 
    ar.AverageRating DESC;"""
curr.execute(sql)
RatingFromRegions = curr.fetchall()
RatingFromRegionsDF = pd.DataFrame(RatingFromRegions, columns = curr.column_names)
RatingFromRegionsDF

# Group by 'Country'  and calculate the average 'AverageRating' for each group
best_performing_region = RatingFromRegionsDF.groupby(['Country','City'])[['AverageRating']].agg('mean').sort_values(by='AverageRating', ascending=False).reset_index()
best_performing_region


# AgeGroup based on Age
def AgeGroup(Age):
    if Age >= 18 and Age <= 29:
        return 'YoungAdults'
    elif Age >= 30 and Age <=44:
        return 'Adults'
    elif Age >= 45 and Age <= 59:
        return 'AgedAdults'
    else:
        return 'OlderAdults'

# Add feedback based on rating  using .apply() method can be used to apply a function to each row of a DataFrame.
RatingFromRegionsDF['AgeGroup'] = RatingFromRegionsDF['Age'].apply(AgeGroup)
RatingFromRegionsDF

# Customer Segment Analysis by Age
AgeGroup_Performance = RatingFromRegionsDF.groupby(['AgeGroup'])[['AverageRating']].agg('mean').sort_values(by='AverageRating', ascending=False).reset_index()
AgeGroup_Performance

# Group by 'Country' and 'ProductID', and calculate the average 'AverageRating' for each group
best_performing_products = RatingFromRegionsDF.groupby(['Country', 'City', 'ProductID'])[['AverageRating']].agg('mean').sort_values(by='AverageRating', ascending=False).reset_index()
best_performing_products.head()
best_performing_products.tail()

# Find Customer is first time buyer or repeated buyer
sql="""SELECT 
    distinct CustomerID,
    CASE 
        WHEN COUNT(VisitDate) OVER (PARTITION BY CustomerID) > 1 THEN 'Repeat Buyer'
        ELSE 'First-Time Buyer'
    END AS BuyerType
FROM 
    customer_journey
    -- WHERE Action = 'Purchase'
ORDER BY 
    CustomerID;"""
curr.execute(sql)
BuyerType = curr.fetchall()
BuyerTypeDF = pd.DataFrame(BuyerType, columns = curr.column_names)
BuyerTypeDF

BuyerTypeCount = BuyerTypeDF[['BuyerType']].value_counts().reset_index()
BuyerTypeCount

