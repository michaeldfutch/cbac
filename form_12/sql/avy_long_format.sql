with 
avy1 as(
    select 
        forecast_zone,
        estimated_avalanche_date as estimated_avalanche_date,
        location as location,
        REGEXP_EXTRACT(start_zone_elevation, r'\w+') as start_zone_elevation,
        number_of_avalanches as number_of_avalanches,
        aspect as aspect,
        type as type,
        failure_plane as failure_plane,
        trigger as trigger,
        trigger_modifier as trigger_modifier,
        was_anyone_caught_in_an_avalanche as was_anyone_caught_in_an_avalanche,
        number_of_people_caught as number_of_people_caught,
        was_anyone_buried as was_anyone_buried,
        number_of_full_burials as number_of_full_burials,
        number_of_partial_burials as number_of_partial_burials,
        'D' || REGEXP_REPLACE(destructive_size, r'[^(\d+\.\d+)]','') as destructive_size,
        'R' || REGEXP_REPLACE(relative_size, r'[^(\d+\.\d+)]','') as relative_size,     
        avg_crown_size_inches as average_crown_height,
        avg_vertical_run_feet as average_vertical_run,
        avg_width_feet as average_width,
        additional_comments as comments
    from `cbac-306316.cbac_wordpress.obs_form_12_direct` direct
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on direct.id = posts.entry_id
    where aspect is not null
),

avy2 as (
    select
        forecast_zone,
        estimated_avalanche_date_1 as estimated_avalanche_date,
        location_1 as location,
        REGEXP_EXTRACT(start_zone_elevation_1, r'\w+') as start_zone_elevation,
        number_of_avalanches_1 as number_of_avalanches,
        aspect_1 as aspect,
        type_1 as type,
        failure_plane_1 as failure_plane,
        trigger_1 as trigger,
        trigger_modifier_1 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_1 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_1 as number_of_people_caught,
        was_anyone_buried_1 as was_anyone_buried,
        number_of_full_burials_1 as number_of_full_burials,
        number_of_partial_burials_1 as number_of_partial_burials,
        'D' || REGEXP_REPLACE(destructive_size_1, r'[^(\d+\.\d+)]','') as destructive_size,
        'R' || REGEXP_REPLACE(relative_size_1, r'[^(\d+\.\d+)]','') as relative_size,     
        avg_crown_size_inches_1 as average_crown_height,
        avg_vertical_run_feet_1 as average_vertical_run,
        avg_width_feet_1 as average_width,
        additional_comments_1 as comments
    from `cbac-306316.cbac_wordpress.obs_form_12_direct` direct
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on direct.id = posts.entry_id
    where aspect_1 is not null
),

avy3 as (
    select
        forecast_zone,
        estimated_avalanche_date_2 as estimated_avalanche_date,
        location_2 as location,
        REGEXP_EXTRACT(start_zone_elevation_2, r'\w+') as start_zone_elevation,
        number_of_avalanches_2 as number_of_avalanches,
        aspect_2 as aspect,
        type_2 as type,
        failure_plane_2 as failure_plane,
        trigger_2 as trigger,
        trigger_modifier_2 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_2 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_2 as number_of_people_caught,
        was_anyone_buried_2 as was_anyone_buried,
        number_of_full_burials_2 as number_of_full_burials,
        number_of_partial_burials_2 as number_of_partial_burials,
        'D' || REGEXP_REPLACE(destructive_size_2, r'[^(\d+\.\d+)]','') as destructive_size,
        'R' || REGEXP_REPLACE(relative_size_2, r'[^(\d+\.\d+)]','') as relative_size,     
        avg_crown_size_inches_2 as average_crown_height,
        avg_vertical_run_feet_2 as average_vertical_run,
        avg_width_feet_2 as average_width,
        additional_comments_2 as comments
    from `cbac-306316.cbac_wordpress.obs_form_12_direct` direct
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on direct.id = posts.entry_id
    where aspect_2 is not null
    ) ,

avy4 as (
    select
        forecast_zone,
        estimated_avalanche_date_3 as estimated_avalanche_date,
        location_3 as location,
        REGEXP_EXTRACT(start_zone_elevation_3, r'\w+') as start_zone_elevation,
        number_of_avalanches_3 as number_of_avalanches,
        aspect_3 as aspect,
        type_3 as type,
        failure_plane_3 as failure_plane,
        trigger_3 as trigger,
        trigger_modifier_3 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_3 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_3 as number_of_people_caught,
        was_anyone_buried_3 as was_anyone_buried,
        number_of_full_burials_3 as number_of_full_burials,
        number_of_partial_burials_3 as number_of_partial_burials,
        'D' || REGEXP_REPLACE(destructive_size_3, r'[^(\d+\.\d+)]','') as destructive_size,
        'R' || REGEXP_REPLACE(relative_size_3, r'[^(\d+\.\d+)]','') as relative_size,     
        avg_crown_size_inches_3 as average_crown_height,
        avg_vertical_run_feet_3 as average_vertical_run,
        avg_width_feet_3 as average_width,
        additional_comments_3 as comments
    from `cbac-306316.cbac_wordpress.obs_form_12_direct` direct
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on direct.id = posts.entry_id
    where aspect_3 is not null
) ,
avy5 as (
    select
        forecast_zone,
        estimated_avalanche_date_4 as estimated_avalanche_date,
        location_4 as location,
        REGEXP_EXTRACT(start_zone_elevation_4, r'\w+') as start_zone_elevation,
        number_of_avalanches_4 as number_of_avalanches,
        aspect_4 as aspect,
        type_4 as type,
        failure_plane_4 as failure_plane,
        trigger_4 as trigger,
        trigger_modifier_4 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_4 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_4 as number_of_people_caught,
        was_anyone_buried_4 as was_anyone_buried,
        number_of_full_burials_4 as number_of_full_burials,
        number_of_partial_burials_4 as number_of_partial_burials,
        'D' || REGEXP_REPLACE(destructive_size_4, r'[^(\d+\.\d+)]','') as destructive_size,
        'R' || REGEXP_REPLACE(relative_size_4, r'[^(\d+\.\d+)]','') as relative_size,     
        avg_crown_size_inches_4 as average_crown_height,
        avg_vertical_run_feet_4 as average_vertical_run,
        avg_width_feet_4 as average_width,
        additional_comments_4 as comments
    from `cbac-306316.cbac_wordpress.obs_form_12_direct` direct
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on direct.id = posts.entry_id
    where aspect_4 is not null
),

union_stmt as (
    select * from avy1
    union all 
    select * from avy2 
    union all 
    select * from avy3 
    union all 
    select * from avy4 
    union all 
    select * from avy5
)

select union_stmt.* 
from union_stmt

    