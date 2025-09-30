
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_co2_kgs
from "emissions"."main"."green_trips_transformed"
where trip_co2_kgs is null



  
  
      
    ) dbt_internal_test