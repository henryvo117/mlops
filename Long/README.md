# MLOPS PROJECT

## Authors

- Luu Hoang Long Vo (luu-hoang-long.vo@epita.fr)
- Phu Hien Le (phu-hien.le@epita.fr)

## Project

To generate fake data
```
python3 sensor.py
```

To setup kafka and zookeeper
```
docker-compose up
```

To start the stream producer
```
python3 stream_producer.py
```

To start the spark structured streaming stream consumer
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 stream_consumer.py
```

To run the analysis without airflow setup in your system
```
spark-submit analysis.py
```

To run the data visualization dashboard 

```
streamlit run data_visualization.py
```
