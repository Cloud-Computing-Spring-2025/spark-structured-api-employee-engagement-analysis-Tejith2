from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Step 1: Create a Spark Session
spark = SparkSession.builder \
    .appName("Employee Engagement Analysis") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()


# Step 2: Load the CSV File
df = spark.read.csv("employee_data.csv", header=True, inferSchema=True)

# Step 3: Check Data Schema
df.printSchema()

# Step 4: Identify Departments with High Satisfaction and Engagement
satisfied_high_engagement = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
dept_analysis = satisfied_high_engagement.groupBy("Department").count()

# Get total employees per department
total_employees_per_dept = df.groupBy("Department").count()

# Calculate percentage
from pyspark.sql.functions import col

# Get total employees per department and rename count column
total_employees_per_dept = df.groupBy("Department").count().withColumnRenamed("count", "total_count")

# Get employees with high satisfaction and engagement, then count per department
satisfied_high_engagement = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
dept_analysis = satisfied_high_engagement.groupBy("Department").count().withColumnRenamed("count", "filtered_count")

# Join both DataFrames to compute percentage
dept_percentage = dept_analysis.join(total_employees_per_dept, "Department") \
    .withColumn("Percentage", (col("filtered_count") / col("total_count")) * 100) \
    .select("Department", "Percentage")

# Display the results
print("Departments with High Satisfaction and Engagement:")
dept_percentage.show()



# Step 5: Employees Who Feel Valued but Didn’t Suggest Improvements
valued_no_suggestions = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))
total_employees = df.count()

num_employees = valued_no_suggestions.count()
proportion = (num_employees / total_employees) * 100

print(f"Number of Employees Feeling Valued without Suggestions: {num_employees}")
print(f"Proportion: {proportion:.2f}%")

# Step 6: Compare Engagement Levels Across Job Titles
job_title_engagement = df.groupBy("JobTitle").agg(avg("SatisfactionRating").alias("AvgEngagementLevel"))
print("Engagement Levels Across Job Titles:")
job_title_engagement.show()

# Step 7: Stop Spark Session
spark.stop()

