
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tpep_dropoff_datetime
from "emissions"."main"."yellow_trips_transformed"
where tpep_dropoff_datetime is null



  
  
      
    ) dbt_internal_test