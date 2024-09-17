import os 
import datetime 

def main_task(path,save_path):
    print('------------------------')
    print('Read data from HDFS')
    print('------------------------')
    df = spark.read.json(path+'20220401'+'.json')
    print('----------------------')
    print('Showing data structure')
    print('----------------------')
    df.printSchema()
    df = df.select('_source.*')
    print('------------------------')
    print('Transforming data')
    print('------------------------')
    df = df.withColumn("Type",
           when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
          .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
                 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
          .when((col("AppName") == 'RELAX'), "Giải Trí")
          .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
          .when((col("AppName") == 'SPORT'), "Thể Thao")
          .otherwise("Error"))
    df = df.select('Contract','Type','TotalDuration')
    df = df.filter(df.Contract != '0' )
    df = df.filter(df.Type != 'Error')
    df = df.groupBy('Contract','Type').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)','TotalDuration')
    print('-----------------------------')
    print('Pivoting data')
    print('-----------------------------')
    #result = pivot_data(df)
    result =  df.groupBy('Contract').pivot('Type').sum('TotalDuration')
    result = result.withColumn('Date',lit('2022-04-01'))
    print('-----------------------------')
    print('Showing result output')
    print('-----------------------------')
    result.show(10,truncate=False)
    print('-----------------------------')
    print('Saving result output')
    print('-----------------------------')
    result.repartition(1).write.csv(save_path,header=True)
    return print('Task Ran Successfully')
	
	
def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value

def date_range(start_date,end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list 

def generate_date_range(from_date,to_date):
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)
	date_list = date_range(from_date,to_date)
	return date_list 
	
    
path =  'C:\\Users\\maxtr\\OneDrive\\Big_Data_Analytics\\Dataset\\log_content'
list_files = os.listdir(path)


def generate_date_range2(fromdate,to_date):
    #fromdate = input('input from date')
    #to_date = input('input to date')
    day_list = []
    for i in range(int(fromdate),int(to_date)+1):
        day_list.append(i)
        print(i)
    print(day_list)
    return day_list 
    
    
df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_mart",
    driver = "com.mysql.cj.jdbc.Driver",
    dbtable = "rfm_daily_statistics",
    user="root",
    password="").load()
    
spark = SparkSession.builder.config("spark.jars.packages","com.mysql:mysql-connector-j:8.0.30").getOrCreate()
    
df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_mart",
    driver = "com.mysql.cj.jdbc.Driver",
    dbtable = "rfm_daily_statistics",
    user="root",
    password="").mode('append').save()        
        
import os 
os.environ['PYSPARK_SUBMIT_ARGS']='--packages com.microsoft.azure:spark-mssql-connector_2.12'