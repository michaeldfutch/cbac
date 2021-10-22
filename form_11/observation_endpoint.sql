with 
avys as (
    select id
    ,struct(
        estimated_avalanche_date as estimated_avalanche_date,
        location_description as location_description,
        REGEXP_EXTRACT(starting_elevation, r'\w+') as starting_elevation,
        number_of_similar_avalanches as number_of_avalanches,
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
        REGEXP_EXTRACT(sized, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer, r'\w+') as size_r,
        average_crown_height as average_crown_height,
        average_vertical_run as average_vertical_run,
        average_width as average_width,
        additional_comments as comments
        ) avy0
    ,struct(
        estimated_avalanche_date_1 as estimated_avalanche_date,
        location_description_1 as location_description,
        REGEXP_EXTRACT(starting_elevation_1, r'\w+') as starting_elevation,
        number_of_similar_avalanches_1 as number_of_avalanches,
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
        REGEXP_EXTRACT(sized_1, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_1, r'\w+') as size_r,
        average_crown_height_1 as average_crown_height,
        average_vertical_run_1 as average_vertical_run,
        average_width_1 as average_width,
        additional_comments_1 as comments
        ) avy1
            ,struct(
        estimated_avalanche_date_2 as estimated_avalanche_date,
        location_description_2 as location_description,
        REGEXP_EXTRACT(starting_elevation_2, r'\w+') as starting_elevation,
        number_of_similar_avalanches_2 as number_of_avalanches,
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
        REGEXP_EXTRACT(sized_2, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_2, r'\w+') as size_r,
        average_crown_height_2 as average_crown_height,
        average_vertical_run_2 as average_vertical_run,
        average_width_2 as average_width,
        additional_comments_2 as comments
        ) avy2
            ,struct(
        estimated_avalanche_date_3 as estimated_avalanche_date,
        location_description_3 as location_description,
        REGEXP_EXTRACT(starting_elevation_3, r'\w+') as starting_elevation,
        number_of_similar_avalanches_3 as number_of_avalanches,
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
        REGEXP_EXTRACT(sized_3, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_3, r'\w+') as size_r,
        average_crown_height_3 as average_crown_height,
        average_vertical_run_3 as average_vertical_run,
        average_width_3 as average_width,
        additional_comments_3 as comments
        ) avy3
            ,struct(
        estimated_avalanche_date_4 as estimated_avalanche_date,
        location_description_4 as location_description,
        REGEXP_EXTRACT(starting_elevation_4, r'\w+') as starting_elevation,
        number_of_similar_avalanches_4 as number_of_avalanches,
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
        REGEXP_EXTRACT(sized_4, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_4, r'\w+') as size_r,
        average_crown_height_4 as average_crown_height,
        average_vertical_run_4 as average_vertical_run,
        average_width_4 as average_width,
        additional_comments_4 as comments
        ) avy4
            ,struct(
        estimated_avalanche_date_5 as estimated_avalanche_date,
        location_description_5 as location_description,
        REGEXP_EXTRACT(starting_elevation_5, r'\w+') as starting_elevation,
        number_of_similar_avalanches_5 as number_of_avalanches,
        aspect_5 as aspect,
        type_5 as type,
        failure_plane_5 as failure_plane,
        trigger_5 as trigger,
        trigger_modifier_5 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_5 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_5 as number_of_people_caught,
        was_anyone_buried_5 as was_anyone_buried,
        number_of_full_burials_5 as number_of_full_burials,
        number_of_partial_burials_5 as number_of_partial_burials,
        REGEXP_EXTRACT(sized_5, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_5, r'\w+') as size_r,
        average_crown_height_5 as average_crown_height,
        average_vertical_run_5 as average_vertical_run,
        average_width_5 as average_width,
        additional_comments_5 as comments
        ) avy5
            ,struct(
        estimated_avalanche_date_6 as estimated_avalanche_date,
        location_description_6 as location_description,
        REGEXP_EXTRACT(starting_elevation_6, r'\w+') as starting_elevation,
        number_of_similar_avalanches_6 as number_of_avalanches,
        aspect_6 as aspect,
        type_6 as type,
        failure_plane_6 as failure_plane,
        trigger_6 as trigger,
        trigger_modifier_6 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_6 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_6 as number_of_people_caught,
        was_anyone_buried_6 as was_anyone_buried,
        number_of_full_burials_6 as number_of_full_burials,
        number_of_partial_burials_6 as number_of_partial_burials,
        REGEXP_EXTRACT(sized_6, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_6, r'\w+') as size_r,
        average_crown_height_6 as average_crown_height,
        average_vertical_run_6 as average_vertical_run,
        average_width_6 as average_width,
        additional_comments_6 as comments
        ) avy6
            ,struct(
        estimated_avalanche_date_7 as estimated_avalanche_date,
        location_description_7 as location_description,
        REGEXP_EXTRACT(starting_elevation_7, r'\w+') as starting_elevation,
        number_of_similar_avalanches_7 as number_of_avalanches,
        aspect_7 as aspect,
        type_7 as type,
        failure_plane_7 as failure_plane,
        trigger_7 as trigger,
        trigger_modifier_7 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_7 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_7 as number_of_people_caught,
        was_anyone_buried_7 as was_anyone_buried,
        number_of_full_burials_7 as number_of_full_burials,
        number_of_partial_burials_7 as number_of_partial_burials,
        REGEXP_EXTRACT(sized_7, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_7, r'\w+') as size_r,
        average_crown_height_7 as average_crown_height,
        average_vertical_run_7 as average_vertical_run,
        average_width_7 as average_width,
        additional_comments_7 as comments
        ) avy7
            ,struct(
        estimated_avalanche_date_8 as estimated_avalanche_date,
        location_description_8 as location_description,
        REGEXP_EXTRACT(starting_elevation_8, r'\w+') as starting_elevation,
        number_of_similar_avalanches_8 as number_of_avalanches,
        aspect_8 as aspect,
        type_8 as type,
        failure_plane_8 as failure_plane,
        trigger_8 as trigger,
        trigger_modifier_8 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_8 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_8 as number_of_people_caught,
        was_anyone_buried_8 as was_anyone_buried,
        number_of_full_burials_8 as number_of_full_burials,
        number_of_partial_burials_8 as number_of_partial_burials,
        REGEXP_EXTRACT(sized_8, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_8, r'\w+') as size_r,
        average_crown_height_8 as average_crown_height,
        average_vertical_run_8 as average_vertical_run,
        average_width_8 as average_width,
        additional_comments_8 as comments
        ) avy8
            ,struct(
        estimated_avalanche_date_9 as estimated_avalanche_date,
        location_description_9 as location_description,
        REGEXP_EXTRACT(starting_elevation_9, r'\w+') as starting_elevation,
        number_of_similar_avalanches_9 as number_of_avalanches,
        aspect_9 as aspect,
        type_9 as type,
        failure_plane_9 as failure_plane,
        trigger_9 as trigger,
        trigger_modifier_9 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_9 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_9 as number_of_people_caught,
        was_anyone_buried_9 as was_anyone_buried,
        number_of_full_burials_9 as number_of_full_burials,
        number_of_partial_burials_9 as number_of_partial_burials,
        REGEXP_EXTRACT(sized_9, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_9, r'\w+') as size_r,
        average_crown_height_9 as average_crown_height,
        average_vertical_run_9 as average_vertical_run,
        average_width_9 as average_width,
        additional_comments_9 as comments
        ) avy9
            ,struct(
        estimated_avalanche_date_10 as estimated_avalanche_date,
        location_description_10 as location_description,
        REGEXP_EXTRACT(starting_elevation_10, r'\w+') as starting_elevation,
        number_of_similar_avalanches_10 as number_of_avalanches,
        aspect_10 as aspect,
        type_10 as type,
        failure_plane_10 as failure_plane,
        trigger_10 as trigger,
        trigger_modifier_10 as trigger_modifier,
        was_anyone_caught_in_an_avalanche_10 as was_anyone_caught_in_an_avalanche,
        number_of_people_caught_10 as number_of_people_caught,
        was_anyone_buried_10 as was_anyone_buried,
        number_of_full_burials_10 as number_of_full_burials,
        number_of_partial_burials_10 as number_of_partial_burials,
        REGEXP_EXTRACT(sized_10, r'\w+') as size_d,
        REGEXP_EXTRACT(sizer_10, r'\w+') as size_r,
        average_crown_height_10 as average_crown_height,
        average_vertical_run_10 as average_vertical_run,
        average_width_10 as average_width,
        additional_comments_10 as comments
        ) avy10

         from `cbac-306316.cbac_wordpress.obs_form_11_direct`
),

avy_arrays as (
    select id
    ,case when avy10.aspect is not null then 10
    when avy9.aspect is not null then 9
when avy8.aspect is not null then 8
when avy7.aspect is not null then 7
when avy6.aspect is not null then 6
when avy5.aspect is not null then 5
when avy4.aspect is not null then 4
when avy3.aspect is not null then 3
when avy2.aspect is not null then 2
when avy1.aspect is not null then 1
when avy0.aspect is not null then 0 
else null end structs
    ,[avy0, avy1,avy2,avys.avy3,avy4,avy5,avy6,avy7,avy8,avy9,avy10] array10
        ,[avy0, avy1,avy2,avys.avy3,avy4,avy5,avy6,avy7,avy8,avy9] array9
            ,[avy0, avy1,avy2,avys.avy3,avy4,avy5,avy6,avy7,avy8] array8
                ,[avy0, avy1,avy2,avys.avy3,avy4,avy5,avy6,avy7] array7
                    ,[avy0, avy1,avy2,avys.avy3,avy4,avy5,avy6] array6
                        ,[avy0, avy1,avy2,avys.avy3,avy4,avy5] array5
                            ,[avy0, avy1,avy2,avys.avy3,avy4] array4
                                ,[avy0, avy1,avy2,avys.avy3] array3
                                    ,[avy0, avy1,avy2] array2
                                        ,[avy0, avy1] array1
                                            ,[avy0] array0
                                            from avys
)


select direct.id entry_id
    , direct.date_created entry_date
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
    when structs = 5 then array5
    when structs = 6 then array6
    when structs = 7 then array7
    when structs = 8 then array8
    when structs = 9 then array9
    when structs = 10 then array10
            end  avalanche_observations
    , case when structs is null then 0 else structs + 1 end number_of_detailed_observations
    , photo_gallery

    from `cbac-306316.cbac_wordpress.obs_form_11_direct` direct
    join avy_arrays on direct.id = avy_arrays.id
    join `cbac-306316.cbac_wordpress.wp_posts_view` posts on avy_arrays.id = posts.entry_id
    