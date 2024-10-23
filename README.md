# Big Data Management Systems @ CEID
### Technologies: Python3, UXsim, Apache Kafka, Apache Spark, Mongo DB

In this assignment, a typical pipeline for real-time data input, processing, and storage in a NoSQL database (MongoDB) is implemented.

Data Generation: A Python script, based on the results of the uxsim simulator, sends data to a Kafka broker at regular intervals. <br/>
<img align="center" height="200" src="https://raw.githubusercontent.com/toruseo/UXsim/images/gridnetwork_fancy.gif" /> 

Real-time Processing: The data from the Kafka broker is consumed by an Apache Spark implementation, which performs real-time processing on it.

Storage in a NoSQL Database: Both the raw data and its processed form (produced by Spark) are stored in a MongoDB database.
