----Form name: Rapid Convenience Assessment
--- Objective: To asssess whether the vaccination campaign has reached its target
--- Inform link :https://inform.unicef.org/uniceftemplates/635/762
--- To be able to execute the queries below, you'll need access to the following tables 
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. spv_rapid_coverage_assessment_form_vaccination - the table that has the households information on the no. of present in the hh, no.of vaccinate/not vaccinated children

---Labels view
--- When pulling data from Inform, the connector separates the labels from the actual values. Labels are normally defined on the choices worksheet of the XLSForm
--- This view extracts labels from the registry table for the RCA form
create or replace view staging.rca_labels as 
(
-- Extract the json column we want, limit by 1 row since the data has a row for each filled record.
with dd as
(
select 
json -> 'xform:choices' as data 
from templates.registry
where uri = 'spv_rapid_coverage_assessment_form?t=json&v=202301251254'
limit 1
), 
-- Strip down the json to the columns we want, question and label details (this includes the language(s) in the form).
unnest_1 as (
select 
js.key as question, 
js.value as label_details
from dd, jsonb_each(dd.data) as js
), 
-- Strip down the json further this time getting the code (name column in the xlsform)
unnest_2 as (
select unnest_1.question, 
js1.key as code, js1.value as language
from unnest_1, jsonb_each(unnest_1.label_details) as js1
)
-- Since the data has a slash(/) sperator for the choices question, we are reversing the order then get the first object before the first slash then reverse it back.
select 
    reverse(split_part(reverse(unnest_2.question), '/', 1)) question,
    unnest_2.code,
    -- Since languages are dynamic,we add this manually for each of the languages used ("und" is when no language has been specified on the form)
    unnest_2.language ->> 'und' AS label
from unnest_2
);

----Tidy table
---This query creates a tidy table of the RCA form actual values ie children present in the hh, vaccinated children and not vaccinated children
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. spv_rapid_coverage_assessment_form_vaccination - the table that has the households information on the no. of present in the hh, no.of vaccinate/not vaccinated children
--- The following section(s) need to be updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. province_iso2_codes needs to be updated to match the reporting country office iso2 codes
create or replace view staging.rca_actuals as
(
select 
  srcafv.parent_id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  srcafv.age_group_label,
  unnest(array['Present in hh','Present in hh','Vaccinated','Vaccinated','Not vaccinated','Not vaccinated']) as rca_category, ----creates a column that has the list provided,
  unnest(array['Males','Females','Males','Females','Males','Females']) as indicator_category,  ---- creates a column that has the gender list,
  unnest(array[srcafv.present_males,srcafv.present_females,srcafv.vaccinated_males,srcafv.vaccinated_females,srcafv.not_vaccinated_males,srcafv.not_vaccinated_females]) as indicator_value, ----creates a column that has the actual values of the fields specified,
  pic.iso2_code
from templates.spv_rapid_coverage_assessment_form_vaccination srcafv 
left join templates.spv_rapid_coverage_assessment_form srcaf  on srcafv.parent_id=srcaf.id  --- Adds the fields associated to the repeat group data
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---adds the vaccine labels from the labels table
left join csv.province_iso2_codes pic on pic.admin2_id::text=a2.name::text ---adds the admin 2 isocodes to be used for the country maps on superset
);


----This query creates a view that has the no.of doors visited, whether mop-up is needed and number of children not vaccinated in the doors visited
--- To be able to perform a sum of the doors visited and no. of children not vaccinated within the visuals this view had to be created
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. province_iso2_codes needs to be updated to match the reporting country office iso2 codes
create or replace view staging.rca as
(
select 
  srcaf.id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  srcaf.doors_visited,
  srcaf.rca_mop_up,
  rl1.label as conducted_by,
  sum(ra.indicator_value) as total_not_vaccinated,
  pic.iso2_code,
  srcaf.submitted_at,
  srcaf.modified_at,
  srcaf.enumerator 
from templates.spv_rapid_coverage_assessment_form srcaf 
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---adds the vaccine labels from the labels table
left join staging.rca_labels rl1 on rl1.code=srcaf.conducted_by and rl1.question='conducted_by' ---adds the conducted by labels from the labels table
left join staging.rca_actuals ra on srcaf.rca_date=ra.rca_date and rl.label=ra.vaccine_administered and a5.label=ra.admin5 and ra.rca_category='Not vaccinated' ---adds the not vaccinated no. of children from the actuals view
left join csv.province_iso2_codes pic on pic.admin2_id::text=a2.name::text ---adds the admin 2 isocodes to be used for the country maps on superset
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15
);

----This query creates a view of the RCA sources of information
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest source_info and no_doors section can be updated if the list of sources of information is longer than the one provided
create or replace view staging.rca_sources_info as
(
with sources_info as 
(
select 
  srcaf.id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  unnest(array['Radio','Tv','Flyers','Other sources of information','Streamer/Tarpaulin','Officials','Social media','Health workers','Relatives/Neighbours']) as sources_info,
  unnest(array[radio,tv,flyers,"others",streamer,officials,social_media,health_workers,relatives_neighbours]) as no_doors
from templates.spv_rapid_coverage_assessment_form srcaf 
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---Adds the vaccine label from the labels column
)
select * from sources_info
where no_doors is not null ---filters out rows with no responses
);


----This query creates a view of the RCA reasons breakdown as to why the children have not been vaccinated 
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest reasons and reasons_value section can be updated if the list of reasons is longer than the one provided
create or replace view staging.rca_not_vaccinated_reasons as
(
with reasons_breakdown as 
(
select 
  srcafv.parent_id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  unnest(array['Reason 1','Reason 2','Reason 3','Reason 4','Reason 5','Reason 6','Reason 7','Reason 8','Reason 9','Reason 10','Other']) as reasons, ---creates a column of the list provided
  unnest(array[reason_1,reason_2,reason_3,reason_4,reason_5,reason_6,reason_7,reason_8,reason_9,reason_10,other_reasons]) as reasons_value --- creates a column that has the responses provided under the respective listed fields
from templates.spv_rapid_coverage_assessment_form_vaccination srcafv 
left join templates.spv_rapid_coverage_assessment_form srcaf on srcaf.id=srcafv.parent_id
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---Adds the vaccine label
)
select * from reasons_breakdown
where reasons_value is not null
);



------POWER BI VIEWS
---The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema
create or replace view public.rca_actuals as 
(
select * from staging.rca_actuals
);


create or replace view public.rca as 
(
select 
    submission_id,
    rca_date,
    admin1,
    admin2,
    admin3,
    admin4,
    latitude,
    longitude,
    admin5,
    vaccine_administered,
    doors_visited,
    rca_mop_up,
    conducted_by,
    total_not_vaccinated,
    iso2_code,
    submitted_at::date,
    modified_at::date,
    enumerator
from staging.rca
);


create or replace view public.rca_sources_info as 
(
select * from staging.rca_sources_info
);


create or replace view public.rca_not_vaccinated_reasons as 
(
select * from staging.rca_not_vaccinated_reasons
);