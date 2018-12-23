from __future__ import absolute_import

import argparse
import logging

import numpy as np
import apache_beam as beam
from google.cloud import storage 
from apache_beam.pvalue import AsList
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

spikey_schema = 'order_id:STRING,zscore:FLOAT'


class CollectOrderSize(beam.DoFn):
    def process(self, element):
        order_id,order_size = element.split(",")
        print('order:',order_size)
        order_size = float(order_size)
        return [order_size]

class CollectOrderTuple(beam.DoFn):
    def process(self, element):
        order_id,order_size = element.split(",")
        order_size = float(order_size)
        order_id = str(order_id)
        return [(order_id,order_size)]


class StandardDeviation(beam.CombineFn):
    def create_accumulator(self):
        return (0.0, 0.0, 0) # x, x^2, count

    def add_input(self, sum_count, input):
        (sum, sumsq, count) = sum_count
        print('sum',sum)
        return sum + input, sumsq + input*input, count + 1

    def merge_accumulators(self, accumulators):
        sums, sumsqs, counts = zip(*accumulators)
        print('sums:',sums)
        return sum(sums), sum(sumsqs), sum(counts)

    def extract_output(self, sum_count):
        (sum, sumsq, count) = sum_count
        if count:
            mean = sum / count
            variance = (sumsq / count) - mean*mean
            stddev = np.sqrt(variance) if variance > 0 else 0
            print('stddev:',stddev)
            stats = {
                'mean': mean,
                'variance': variance,
                'stddev': stddev,
                'count': count
            }
            print(stats)
            return stats
            
   
def calculate_zscore(cur_order,past_stats):
    
    past_stats = past_stats[0]
    order_id,cur_order_size = cur_order
    z_score = (cur_order_size - past_stats['mean'] ) / past_stats['stddev']
    print(z_score)
    print(order_id)
    return {
        'order_id': order_id,
        'zscore': z_score,
    }

    

def update_csv(current_order):

    STORAGE_BUCKET = 'spikey-asia-storage'
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(STORAGE_BUCKET)
    
    blob = bucket.blob('inputs/orders.csv')
    
    if blob.exists():
        blob.download_to_filename('/tmp/orders.csv')
    
    with open('/tmp/orders.csv','a+') as file:
        file.write(str(current_order+'\n'))
        file.close()
    
    blob.upload_from_filename('/tmp/orders.csv')
    print('file Uploaded!')
    
    


def run(argv=None):
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--input_file',
      required=True,
      help=('Input file in the form of gcs bucket url'))

  parser.add_argument(
      '--input_topic',
      required=True,
      help=('Input PubSub topic of the form '))

                   
  known_args, pipeline_args = parser.parse_known_args(argv)

  
  pipeline_options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=pipeline_options)

  pipeline_options.view_as(SetupOptions).save_main_session = True
  
  pipeline_options.view_as(StandardOptions).streaming = True
 
  live_sales =( 
               p |'Read From PubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
                                               .with_output_types(bytes)
                 | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
         )
         
  historical_sales = (
            p | beam.io.ReadFromText(known_args.input_file, skip_header_lines=1) 
        )
  
  order_stats = (
            historical_sales | 'Split & Collect order_size' >> (beam.ParDo(CollectOrderSize()))
                      | 'Calcuate Mean & Standard Deviation' >> beam.CombineGlobally(StandardDeviation())
            )
            
  current_asp = (       
                live_sales   
                              |'Get Current Order size' >> (beam.ParDo(CollectOrderTuple())) 
                              | 'Calcuate Z-score' >> beam.Map(calculate_zscore,AsList(order_stats))
                              | 'Write to Bigquery' >> beam.io.WriteToBigQuery('spikey-gcp:spikey_orders.order_zscores', schema=spikey_schema)
             )
  

  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
  
  
  