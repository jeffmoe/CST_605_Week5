# CST_605_Week5
Repo for the Week 5 assignment regarding data streaming.

Use the main.py file which calls the other files into use. Created on a Debian instance of WSL2 in windows 11. Utilized a Docker kafka container for streaming data with a weather stack api. 

Actions:
$ docker run -d --name=kafka -p 9092:9092 apache/kafka
$ python main.py --api-key YOUR_API_KEY_HERE --mode once --num-readings 10 --reading-interval 30
This will run the kafka processor, consumer, and visualize the current weather data (free tier) from weatherstack.com. It will collect 10 readings, once every 30 seconds.
Kafka was hosted locally on port 9092.
