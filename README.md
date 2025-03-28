# DataScience_Project_CustomerBehaviorAnalysis
Providing a data-based solution to evaluate customer behavior and improve the customer experience for ShopEasy's marketing strategy.

# Problem Statement :
ShopEasy, an online retail business, is experiencing a decline in customer engagement and conversion rates, despite investing heavily in marketing campaigns. The company has observed:

**Reduced Customer Interactions**: Fewer customers are engaging with the website and marketing content.
**Decreased Conversion Rates**: A decline in the percentage of site visitors making purchases.
**High Marketing Expenses**: Increased spending on digital marketing but without a proportional increase in revenue.
**Customer Feedback Gaps**: Poorly understood customer preferences due to lack of structured feedback analysis.


To begin the analysis, we require the dataset, which has been provided by the client. The data includes the following tables: **customers, customer_reviews, customer_journey, engagement_data, geography, and products.**

First I took was to upload all the tables into a MySQL database using a Python script. Following that, I formatted the tables into proper structures and split the column values in  the Engagement_data and formatted the values in Customer_journey tables for easier analysis.


The **first step** in my analysis was to examine the factors influencing customer engagement. I began by analyzing the Engagement_data table to assess engagement levels based on content type. 
Following this, I investigated the drop-off rate at each stage. I then analyzed customer engagement by campaigns. Next, I evaluated the total duration spent at each stage and action-wise. This series of analyses provided valuable insights into the factors driving customer engagement.


The **second step** in my analysis was to examine the outcomes of customer reviews. I began by calculating the average rating from the customer_review table and then performed a merge operation with the product table. This allowed me to associate ratings with each product.
Subsequently, I analyzed the feedback based on customer ratings and I aggregated the sum of responses for each product ID. 
Finally, I analyzed the engagement levels of each product ID in relation to their average ratings. 
This sequence of analyses provided meaningful insights into customer review outcomes.


The **third step** in my analysis was to examine the performance of products based on customer regions. Initially, I performed a merge operation between the customer and geography tables. After obtaining the merged results, I further merged the customer_review table to calculate the average rating for each product. This helped identify the ratings and performance of products across different regions.
Subsequently, I analyzed product ratings based on customer age groups and identified the top and bottom-rated products across regions. In the final step, I did an analysis focusing on the number of repeat customers.


The results derived from the above analysis provide valuable insights into customer sentiments, areas of concern, opportunities for improvement, the impact of negative reviews on product performance, and innovative strategies to enhance customer satisfaction.
