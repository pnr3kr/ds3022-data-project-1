
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_mph
from "emissions"."main"."yellow_trips_transformed"
where avg_mph is null



  
  
      
    ) dbt_internal_test