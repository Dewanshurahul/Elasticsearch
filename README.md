# Elasticsearch 

Elasticsearch can be used to search all kinds of documents. It provides scalable search, has near real-time search, and supports multitenancy. "Elasticsearch is distributed, which means that indices can be divided into shards.

Source for DATA :-
http://eforexcel.com/wp/downloads-16-sample-csv-files-data-sets-for-testing/

The problem is to injest data from Spark into ElasticSearch and slove the given Problem Statements using the given Data Set

  - The dataset consists of 37 columns and 2M Rows from which some of them are used for Solving the Problem
  - 5th column is Gender
  - 25th column is Salary
  - 26th column is Last % Hike
  - 30th column is Country
  - 31st column is City
  - 34th column is Region

 - I have Created a Task Class into which my all business logic are Present in file name task.py
 
 - And also a python file name context.py into which SparkContext and Data Reading from hdfs function resides into Context Class

 - And an es.py file for all the connections and elasticsearch operations.

 - And a runner.py file is there which is responsible for executing all the business logic
 
 - Command to execute runner.py is :-
```sh
$ python runner.py
```
Question 1 :- Count number of employees in each Region, Country and City.
Solution :- The solution for this Question is divided into three parts
 - 1st :- For Region
 - - I have created a function region in task.py file in Task Class
 - 2nd :- For Country
 - - I have created a function country in task.py file in Task Class
 - 3rd :- For City
 - - I have created a function city in task.py file in Task Class

OutPut Image :- 
<img src=outputImages/task-1.png>

Question 2:- Summary for Employee
Solution - I have Created a function summary in task.py file in Task Class

OutPut Image :- 
<img src=outputImages/task-2.png>

Question 3:- Orderby Gender and Salary
Solution :- I have created a function orderby_gender_salary in task.py file in Task Class

OutPut Image :- 
<img src=outputImages/task-3.png>

Question 4 :- Summarise the number of emp joined and hickes granted based on month
Solution :- The solution for this Question is divided into three parts
 - 1st :- For Hikes Granted Based on Month
 - - I have created a function hikes_granted_based_on_month in task.py file in Task Class
 - 2nd :- For Employee Joined based on Month
 - - I have created a function employee_joined_based_on_month in task.py file in Task Class

OutPut Image :- 
<img src=outputImages/task-4.png>

Question 5 :- Salary wise sort employee data
Solution :- I have created a function sort_salary in task.py file in Task Class

OutPut Image :- 
<img src=outputImages/task-5.png>