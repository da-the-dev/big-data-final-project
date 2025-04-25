#!/bin/bash

curl -L -o data/us-traffic-congestions-2016-2022.zip https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-traffic-congestions-2016-2022

unzip data/us-traffic-congestions-2016-2022.zip -d data/
mv data/us-traffic-congestions-2016-2022/us_congestion_2016_2022.csv data/
rm -rf data/us-traffic-congestions-2016-2022
rm data/us-traffic-congestions-2016-2022.zip
