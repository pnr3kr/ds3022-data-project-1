
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lpep_pickup_datetime
from "emissions"."main"."green_trips_transformed"
where lpep_pickup_datetime is null



  
  
      
    ) dbt_internal_test