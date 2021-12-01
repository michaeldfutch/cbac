with 
avys as (
    select id
    ,struct(
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
        ) avy0
    ,struct(
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
        ) avy1
            ,struct(
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
        ) avy2
            ,struct(
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
        ) avy3
            ,struct(
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
        ) avy4

         from `cbac-306316.cbac_wordpress.obs_form_12_direct`
),

avy_arrays as (
    select id
    ,case 
when avy4.aspect is not null then 4
when avy3.aspect is not null then 3
when avy2.aspect is not null then 2
when avy1.aspect is not null then 1
when avy0.aspect is not null then 0 
else null end structs
                            ,[avy0, avy1,avy2,avys.avy3,avy4] array4
                                ,[avy0, avy1,avy2,avys.avy3] array3
                                    ,[avy0, avy1,avy2] array2
                                        ,[avy0, avy1] array1
                                            ,[avy0] array0
                                            from avys
)


select direct.id entry_id
    , direct.date_created entry_date
    , direct.date_updated date_updated
    , subject
    , forecast_zone forecast_zone_top
    , weather
    , route_description
    , snowpack
    , danger
    , problems
    , risk_management
    , did_you_see_any_avalanches
    , case when structs is null then []
    when structs = 0 then array0
    when structs = 1 then array1
    when structs = 2 then array2
    when structs = 3 then array3
    when structs = 4 then array4
            end  avalanche_observations
    , case when structs is null then 0 else structs + 1 end number_of_detailed_observations
    , photo_gallery

    from `cbac-306316.cbac_wordpress.obs_form_12_direct` direct
    join avy_arrays on direct.id = avy_arrays.id
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on avy_arrays.id = posts.entry_id  