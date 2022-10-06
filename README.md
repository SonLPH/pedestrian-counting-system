# pedestrian-counting-system
 
## Resources ##
* https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Monthly-counts-per-hour/b2ak-trbp
* https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234

## Architecture on AWS ##
* Data lake: AWS S3 Bucket
* Hadoop Cluster: AWS EMR
* Linux Instance: AWS EC2

Workflow on AWS
![image](https://user-images.githubusercontent.com/112694749/194244564-b4fb1603-1202-4992-8c54-c577d188b627.png)

## Installation ##
### For running spark job on local ###
1. Move to processing directory
```
cd pedetrian-counting-system\data-processing
```
2. Run .py file or using Jupyter Notebook
```
spark-submit .\pedestrian_counting_spark.py -ipc "input directory" -o "output directory"
```
### For running spark job on AWS cloud ###
1. Upload file pedestrian_counting_spark.py and data to S3 Bucket
2. Create EMR Cluster
3. Connect to EMR through SSH
4. Run spark job and save the result to S3 Bucket
```
spark-submit "file .py directory" -ipc "input directory" -o "output directory"
```
