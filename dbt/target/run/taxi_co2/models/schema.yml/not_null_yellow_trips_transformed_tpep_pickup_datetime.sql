
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tpep_pickup_datetime
from "emissions"."main"."yellow_trips_transformed"
where tpep_pickup_datetime is null



  
  
      
    ) dbt_internal_test