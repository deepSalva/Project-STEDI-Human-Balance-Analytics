# Data Lakehouse for IoT Data Ingestion

This repository is a use case for developing a Data lakehouse in Amazon Web Service (AWS).  For that we are going to implement Spark jobs 
in AWS Glue, that allows us to process data from multiple sources, categorize the data, and curate it to be queried in 
the future for multiple purposes. 

The use case is based on a company STEDI that analyze sensor data from the STEDI team to build a data lakehouse solution
for sensor data that trains a machine learning model. The STEDI Team has been hard at work developing a hardware 
STEDI Step Trainer that:
* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.

The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone 
accelerometer to detect motion in the X, Y, and Z directions. The STEDI team wants to use the motion sensor data to 
train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in
deciding what data can be used. Some early adopters have agreed to share their data for research purposes. 
Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine 
learning model.

We will be representing the Data Engineer role for the STEDI team to extract the data produced by the STEDI Step 
Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists 
can train the learning model. This project is based on the Project: STEDI Human Balance Analytics in the Data Engineer 
course, Udacity.

This entire project was written and run in AWS Glue Studio and Athena. For you to create your own project you will need
access and resources in AWS. 

Prerequisites:
* Python mid level
* AWS environment: Create an account, IAM, s3 and CLI management basics.
* Spark and DAG paradigm understanding.
* Confortable with SQL basic operations. 
* Basic concepts for Data Lake and Data Lakehouse
* Athena and [Glue Studio](https://docs.aws.amazon.com/glue/latest/ug/tutorial-create-job.html) basics

## Environment 

We are using the data from the STEDI Step Trainer and mobile app to develop a lakehouse solution in the cloud 
that curates the data for the machine learning model using:

* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3

## Project Data

STEDI has three JSON data sources to use from the Step Trainer. You can download 
the data [from here](https://video.udacity-data.com/topher/2022/June/62be2ed5_stedihumanbalanceanalyticsdata/stedihumanbalanceanalyticsdata.zip)

1. **Customer Records (from fulfillment and the STEDI website):**

* serialnumber
* sharewithpublicasofdate
* birthday
* registrationdate
*  sharewithresearchasofdate
*  customername
*  email
* lastupdatedate
*  phone
*  sharewithfriendsasofdate

2. **Step Trainer Records (data from the motion sensor):**

* sensorReadingTime
* serialNumber
*  distanceFromObject

3. **Accelerometer Records (from the mobile app):**

* timeStamp
*  serialNumber
*  x
*  y
*  z

## Project Objectives

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS 
that satisfies these requirements from the STEDI data scientists. 

### Tasks

1. To simulate the data coming from the various sources, we need to create our own S3 directories for 
customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point. 


2. create AWS Glue Jobs that do the following:
* Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share 
their data for research purposes (Trusted Zone) - creating a Glue Table called **customer_trusted**.
* Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from 
customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.
* Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who 
have accelerometer data and have agreed to share their data for research called customers_curated. 
* Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that 
contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data 
for research (customers_curated).

## Repository files

In this repository you can find:
* Spark_Python scripts generated for the Glue Jobs
* SQL statements for the tables creation in Athena
* Images with table query examples