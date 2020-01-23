
This project was developed with the aim of building a dashboard with orders created during the black friday (2019).
New orders created while the app was running would be transformed and generate one tabular data that
would be the input to the visualization on dashboard. 


## Run locally

To run locally, best advise is to use an IDE and sbt (dependencies management tool for Scala projects). 
On `/src/main/scala/Main.scala`, change the values for variables inputDir and outputDir to absolute local paths.
After starting application, copy some new .parquet files to the inputDir (to simulate new files to be streamed).

## Run on EC2 instances

To run on AWS infrastructure, you can run the `deploy.sh` script. It will deploy the application
on a EMR cluster.   
For running the script, you must edit it to fill the following info:

- cluster_id: EMR cluster id (you must create one EMR cluster before run script).
- app_name: Name for the app to run.
- jar_name: Name for the jar created when your scala project is compiled.
- main_class: the main class on the scala project to execute.
- s3_jar_dir: A s3 path (like s3://my.bucket/dir/) to where upload your scala project jar in order to 
indicate to EMR step where is the app to run. Your machine must have right to write on this bucket. 
