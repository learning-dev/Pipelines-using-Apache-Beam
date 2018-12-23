# Pipelines-using-Apache-Beam

A pipeline using Apache Beam that read a file from **Google Storage Bucket** containing orders, at same time reads a new order from **PubSub**. It calculates the **mean, variance, standard deviation** for the historical data i.e. orders from GCS and calculates the **Z score**  for the new order then writes it to **Big Query**. It also write the current order to bucket i.e. write the input to gcs bucket as a historical data. 

## To run the file

First, generate the csv file full of orders. 

```
python generate_orders.py

```
Now, that you have the csv file upload it to GCS bucket 

Now, run the **csv file generator to get some data to publish through Pubsub**. Change the no_of_orders to 10ish to generatre small csv file. Also change name of the csv file in generator like **csv_pubsub.csv**


Make sure you have google bucket, pubsub topic created and also create a table in Big Query with schema similar to that of specified in python file 


```
python apache_beam.py --input_file gs://bucket-name/inputs/orders.csv \
                --input_topic projects/project-name/topics/topic-name \
                --project project-name\
                --temp_location gs://bucket-name/tmp/
```

To clone this project. 

```
git clone https://github.com/learning-dev/Pipelines-using-Apache-Beam.git
```
