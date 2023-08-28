create database CarBikeDataDB;

use CarDataDB;

select * from car_bike_data;

#rank based on number of Km driven based on fule type
select car_bike_data.*,
rank() over(partition by Fuel_Type order by Kms_Driven desc) as Km_Rank,
dense_rank() over(partition by Fuel_Type order by Kms_Driven desc) as Km_dense_Rank
from car_bike_data;

#rank based on selling price of car and Fuel_Type
select car_bike_data.*,
rank() over(partition by Fuel_Type order by Selling_Price desc) as rank_no
from car_bike_data;

#Find sum/avg/min/max KMs_Driven for each fuel type
select Fuel_Type,Kms_Driven,Car_Name,Year,
min(Kms_Driven) over (partition by Fuel_Type) as Min_Kms_Driven,
max(Kms_Driven) over (partition by Fuel_Type) as Max_Kms_Driven,
sum(Kms_Driven) over (partition by Fuel_Type) as Sum_Kms_Driven,
avg(Kms_Driven) over (partition by Fuel_Type) as Avg_Kms_Driven
from car_bike_data;

#avg Kms_Driven for Manual and Automatic transmission and Fuel_Type
select Car_Name,Kms_Driven,Fuel_Type,Transmission,
avg(Kms_Driven) over(partition by Transmission,Fuel_Type) as Avg_Kms_Driven_Auto_Manual,
sum(Kms_Driven) over(partition by Transmission,Fuel_Type) as Sum_Kms_Driven_Auto_Manual,
min(Kms_Driven) over(partition by Transmission,Fuel_Type) as Min_Kms_Driven_Auto_Manual,
max(Kms_Driven) over(partition by Transmission,Fuel_Type) as Max_Kms_Driven_Auto_Manual
from car_bike_data;

#Find avg/min/max/sum of present price and group them based on fuel_type
select Fuel_Type,Car_Name,Present_Price,
avg(Present_Price)over(partition by Fuel_Type) as Avg_Present_Price,
sum(Present_Price)over(partition by Fuel_Type) as Sum_Present_Price,
min(Present_Price)over(partition by Fuel_Type) as Min_Present_Price,
max(Present_Price)over(partition by Fuel_Type) as Max_Present_Price
from car_bike_data;

#Rank based on Kms_Driven and Transmission and Fuel_Type
select rank() over(partition by Transmission,Fuel_Type,Seller_Type
order by Kms_Driven desc) as Rank_No,car_bike_data.*
from car_bike_data;

#Top 5 cars whoes Km_Driven is maximum based on Fuel_Type ,Seller_Type and Transmission
select * from (
select row_number()
over(partition by Transmission,Fuel_Type,Seller_Type
order by Kms_Driven desc) as Row_no
,car_bike_data.*
from car_bike_data) x where x.Row_no<=5;

#Year wise km_driven 
select car_bike_data.*,
round(avg(Kms_Driven)over(partition by Year,Fuel_Type),2) as Avg_Km_Driven,
sum(Kms_Driven)over(partition by Year,Fuel_Type) as Sum_Km_Driven,
min(Kms_Driven)over(partition by Year,Fuel_Type) as Min_Km_Driven,
max(Kms_Driven)over(partition by Year,Fuel_Type) as Max_Km_Driven
from car_bike_data;

#comapre the avg Kms_Driven based on  Transmission and Seller_Type
select car_bike_data.*,
avg(Kms_Driven) over(partition by Transmission,Seller_Type) as Avg_Kms_Driven_trans,
sum(Kms_Driven) over(partition by Transmission,Seller_Type) as Sum_Km_Driven_trans,
min(Kms_Driven) over(partition by Transmission,Seller_Type) as Min_Km_Driven_trans,
max(Kms_Driven) over(partition by Transmission,Seller_Type) as Max_Km_Driven_trans
from car_bike_data;

#use of lead and lag function  with Next and Previous Kms_Driven
select car_bike_data.*,
lead(Kms_Driven) over(partition by Fuel_Type,Seller_Type order by Kms_Driven desc
) as NextKms,
lag(Kms_Driven) over(partition by Fuel_Type,Seller_Type order by Kms_Driven desc
) as PrevKms
from car_bike_data;


#check Kms_Driven based on Fuel Type,Transmission and Year
select Car_Name,Kms_Driven,Fuel_Type,Transmission,Year,
max(Kms_Driven)over(partition by Fuel_Type,Transmission,Year) as Max_Kms_Driven,
min(Kms_Driven)over(partition by Fuel_Type,Transmission,Year) as Min_Kms_Driven,
avg(Kms_Driven)over(partition by Fuel_Type,Transmission,Year) as  Avg_Kms_Driven,
lead(Kms_Driven)over(partition by Fuel_Type,Transmission,Year) as NextKms_Driven,
lag(Kms_Driven)over(partition by Fuel_Type,Transmission,Year) as PrevKms_Driven
from car_bike_data;

#find Avg/min/max/sum of kms_driven based on Year and Transmission
select car_bike_data.*,
avg(Kms_Driven) over(partition by Year,Transmission) as Avg_Kms_Driven,
sum(Kms_Driven) over(partition by Year,Transmission) as Sum_Kms_Driven,
min(Kms_Driven) over(partition by Year,Transmission) as Min_Kms_Driven,
max(Kms_Driven) over(partition by Year,Transmission) as Max_Kms_Driven
from car_bike_data;

#find Avg/min/max/sum of kms_driven based on Seller_Type,Fuel_Type,Year
select car_bike_data.*,
avg(Kms_Driven) over(partition by Seller_Type,Fuel_Type,Year) as Avg_Kms_Driven,
sum(Kms_Driven) over(partition by Seller_Type,Fuel_Type,Year) as Sum_Kms_Driven,
min(Kms_Driven) over(partition by Seller_Type,Fuel_Type,Year) as Min_Kms_Driven,
max(Kms_Driven) over(partition by Seller_Type,Fuel_Type,Year) as Max_Kms_Driven
from car_bike_data;

#compare avg Kms_Driven group by Owner,Transmission,Fuel_Type,Seller_Type.
select car_bike_data.*,avg(Kms_Driven) 
over(partition by  Owner,Transmission,Fuel_Type,Seller_Type) as AvgKmsDriven
from car_bike_data;

#compare avg Kms_Driven group by Owner,Transmission,Fuel_Type.
select car_bike_data.*,avg(Kms_Driven) 
over(partition by  Owner,Transmission,Fuel_Type) as AvgKmsDriven
from car_bike_data;

#compare avg Kms_Driven group by Owner,Transmission

select car_bike_data.*,avg(Kms_Driven) 
over(partition by  Owner,Transmission) as AvgKmsDriven
from car_bike_data;

#compare avg Kms_Driven group by Owner.
select car_bike_data.*,avg(Kms_Driven) 
over(partition by  Owner) as AvgKmsDriven
from car_bike_data;

#analytical function 
select car_bike_data.*,
lead(Kms_Driven) over() as NextKms,
lag(Kms_Driven) over() as PrevKms,
first_value(Kms_Driven) over() as FirstValue,
last_value(Kms_Driven) over() as LastValue
from car_bike_data;

#ranking function 
select car_bike_data.*,
rank() over(order by kms_Driven desc) as RANK_No,
dense_rank() over(order by kms_Driven desc) as Dense_RANK_No,
row_number() over(order by Kms_Driven desc) as RowNumber,
ntile(3) over(order by Kms_Driven desc) as Rank_Ntile
from car_bike_data;

#Show 10 cars having minimum Kms_Driven 
select * from(
select car_bike_data.*,row_number() over(order by Kms_Driven) as rank_Kms_driven
from car_bike_data) x where x.rank_Kms_driven<=10;

#Show 10 cars having maximum Kms_Driven 
select * from(
select car_bike_data.*,row_number() over(order by Kms_Driven desc) as rank_Kms_driven
from car_bike_data) x where x.rank_Kms_driven<=10;


#Show 10 cars having maximum Selling_Price 
select * from(
select car_bike_data.*, row_number() over(order by Selling_Price desc) as RankNumber
from car_bike_data) x where x.RankNumber<=10;

#Show 10 cars having minimum Selling_Price 
select * from(
select car_bike_data.*, row_number() over(order by Selling_Price) as RankNumber
from car_bike_data) x where x.RankNumber<=10;
