---Form name: Supplemental Immunization Activity
---Objective: This form is used to collect the vaccination campaign actual values. Some of the questions include: no. of children who have been vaccinated per age group and gender, no. of children who have refused/deferred and reason breakdown for refusal and deferral
---Inform link: https://inform.unicef.org/uniceftemplates/635/759
--- The connector normally creates a separate table for the repeat groups and labels that can be joined to the other tables using SQL
-- To be able to execute the queries below, you'll need access to the following tables 
                  -- a. supplemental_immunization_activity - this is the table that has all the responses from the Supplemental Immunization Activity form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. supplemental_immunization_activity_vaccine - the repeat group table that has the vaccination data ie no.of children vaccinated/deferred/refused per age group and gender
                  -- h. supplemental_immunization_activity_target - has the number of vaccines received, campaign start and end date, 
                  -- i. supplemental_immunization_activity_target_target_children - has the no. of children to be vaccinated per age group
--Note: The administrative hierarchy is dependent on the country that is implementing the RT-VaMA tool kit. On queries below, we used Philippines administrative hierarchy as our use case.


---Supplemental Immunization Activity labels table
--- This view extracts labels from the registry table
create or replace view staging.sia_labels as 
(
-- Extract the json column we want, limit by 1 row since the data has a row for each filled record.
with dd as
(
select 
json -> 'xform:choices' as data 
from templates.registry
where uri = 'Supplemental_Immunization_Activity?t=json&v=202302071328'
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


---- SIA Actuals
--- This query creates a view that has the SIA actual vaccinated, refused and deferred values collected across the different facilities per age group and gender
--- There is unnesting in the query so as to create a tidy table that can be used to create different visualization based on the indicators required.
--- For the query to execute successfully, the following tables are required:
       --- a. supplemental_immunization_activity_vaccine - the repeat group table within the SIA form
       --- b. supplemental_immunization_activity - the table with the actual vaccination values; vaccinated children, refused children, deferred children
       --- c. admin1 - this table contains all the admin1 level admin names and codes
       --- d. admin2 - this table contains all the admin2 level admin names and codes
       --- e. admin3 - this table contains all the admin3 level admin names and codes
       --- f. admin4 - this table contains all the admin4 level admin names and codes
       --- g. admin5 - this table contains all the admin5 level admin names and codes
--- The following sections need to updated during customization:
      --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country reporting_office adopting the tool
create or replace view staging.sia_actuals as 
(
select 
  siav.parent_id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  sia.admin4_lat::real as latitude,
  sia.admin4_long::real as longitude,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  siav.age_group_label,
  unnest(array['Vaccinated','Vaccinated','Deferred Vaccinated','Deferred Vaccinated','Refused Vaccinated','Refused Vaccinated','Deferred','Deferred','Refused','Refused']) as coverage_category,
  unnest(array['Males','Females','Males','Females','Males','Females','Males','Females','Males','Females']) as indicator_category,
  unnest(array[vaccinated_males,vaccinated_females,vaccinated_males_previously_deferred,vaccinated_females_previously_deferred,vaccinated_males_previously_refused,vaccinated_females_previously_refused,deferred_males,deferred_females,refused_males,refused_females]) as indicator_value
from templates.supplemental_immunization_activity_vaccine siav 
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id  --- Adds the fields assosciated with the repeat group data
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
);


---This script creates a view that aggregates the actual values to admin 4 level so that the actuals can be matched to the target values
--- This view is useful when calculating the coverage, wastage rate, vials used and discarded
--- For the query to execute successfully, the following tables are required:
       --- a. supplemental_immunization_activity_target - the table with the targets 
       --- b. supplemental_immunization_activity_target_target_children - the repeat group table within the targets form
       --- c. supplemental_immunization_activity_vaccine - the repeat group table within the SIA form
       --- d. supplemental_immunization_activity - the table with the actual vaccination values; vaccinated children, refused children, deferred children
       --- e. admin1 - this table contains all the admin1 level admin names and codes
       --- f. admin2 - this table contains all the admin2 level admin names and codes
       --- g. admin3 - this table contains all the admin3 level admin names and codes
       --- h. admin4 - this table contains all the admin4 level admin names and codes
       --- i. admin5 - this table contains all the admin5 level admin names and codes
--- The following sections need to updated during customization:
      --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country reporting_office adopting the tool
---This sub query creates the SIA targets view
create or replace view staging.sia_targets as 
(
select 
 a1.label as admin1,
 a2.label as admin2,
 a3.label as admin3,
 a4.label as admin4,
 a5.label as admin5,
 siat.campaign_start_date,
 siat.campaign_end_date,
 (siat.campaign_end_date - siat.campaign_start_date) as campaign_days,
 siat.vaccine_label as vaccine_administered,
 siattc.no_children as campaign_target,
 siattc.age_group_label 
from templates.supplemental_immunization_activity_target siat  
left join templates.supplemental_immunization_activity_target_target_children siattc on siattc.parent_id=siat.id 
left join csv.admin1 a1 on siat.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on siat.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on siat.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on siat.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on siat.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
);

----This sub query creates the vaccine dose view
create or replace view staging.sia_vaccine_dose as 
(
select 
 sia.date_vaccination_activity,
 a1.label as admin1,
 a2.label as admin2,
 a3.label as admin3,
 a4.label as admin4,
 a5.label as admin5,
 sia.vaccine_label as vaccine_administered,
 MAX(sia.vial_dosage) as vial_dosage,
 SUM(sia.vials_used) as vials_used,
 SUM(sia.vial_dosage::int*sia.vials_used) as vaccine_dose,
 SUM(sia.vials_discarded) as vials_discarded 
from templates.supplemental_immunization_activity sia
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ----Adds admin 5 labels using the admin name column
group by 1,2,3,4,5,6,7
);


-----The query below creates an aggregate view of the actual vaccinated values, vaccine dose and target values
create or replace view staging.aggregated_sia_actuals_target as
(
with 
targets as 
--Retrieves the campaign target from the sia_targets view
-- The query gets the cumulative value of the campaign target without the age group breakdown 
(
select 
 st.admin5,
 st.vaccine_administered,
 st.campaign_start_date,
 st.campaign_end_date,
 SUM(st.campaign_target) as campaign_target
from staging.sia_targets st
group by 1,2,3,4
),
actuals as 
(
select 
sa.date_vaccination_activity,
sa.admin1,
sa.admin2,
sa.admin3,
sa.admin4,
sa.admin5,
sa.latitude::real,
sa.longitude::real,
sa.vaccine_administered,
SUM(sa.indicator_value) filter (where sa.coverage_category in ('Deferred Vaccinated','Refused Vaccinated','Vaccinated')) as total_vaccinated
from staging.sia_actuals sa
group by 1,2,3,4,5,6,7,8,9
)
---Joins the target values to the actual values without gender, agegroup disaggregation
select 
 hcd.date,
 sa.admin1,
 sa.admin2,
 sa.admin3,
 sa.admin4,
 sa.admin5,
 sa.vaccine_administered,
 sa.total_vaccinated,
 tar.campaign_target,
 vd.vials_used,
 vd.vials_discarded,
 vd.vaccine_dose,
 vd.vial_dosage,
 sa.latitude::real,
 sa.longitude::real
from csv.hard_coded_dates hcd 
left join actuals sa on sa.date_vaccination_activity=hcd.date
left join targets tar on tar.vaccine_administered=sa.vaccine_administered and tar.admin5=sa.admin5 and sa.date_vaccination_activity between tar.campaign_start_date and tar.campaign_end_date 
left join staging.sia_vaccine_dose vd on vd.date_vaccination_activity=hcd.date and vd.vaccine_administered=sa.vaccine_administered and sa.admin5=vd.admin5 --matches the value at reporting date, vaccine and admin5 level
where hcd.date<=now()::date and sa.date_vaccination_activity is not null ----Filters dates that are not within the actuals form
);

---This script creates a view that gets the actual and the target vaccination values from the SIA and SIA targets form 
--- This view is useful when getting the no. of children who are remaining to be vaccinated, who have remained within the deferred and refused groups after some have been vaccinated
--- For the query to execute successfully, the following tables are required:
       --- a. supplemental_immunization_activity_target - the table with the targets 
       --- b. supplemental_immunization_activity_target_target_children - the repeat group table within the targets form
       --- c. supplemental_immunization_activity_vaccine - the repeat group table within the SIA form
       --- d. supplemental_immunization_activity - the table with the actual vaccination values; vaccinated children, refused children, deferred children
       --- e. hard_coded_dates - the table that has listed dates from 1st December 2022 to 25th August 2025
       --- f. admin1 - this table contains all the admin1 level admin names and codes
       --- g. admin2 - this table contains all the admin2 level admin names and codes
       --- h. admin3 - this table contains all the admin3 level admin names and codes
       --- i. admin4 - this table contains all the admin4 level admin names and codes
       --- j. admin5 - this table contains all the admin5 level admin names and codes
--- The following sections need to updated during customization:
      --- a. hard_coded_dates needs to be updated if the dates will have surpassed 25th August 2025
      --- b. province_iso2_codes needs to be updated to match the country office iso2 codes
create or replace view staging.sia_actuals_target as
(
with actuals as 
---Retrieves the actual values from the vaccination coverage group
(
select 
  sa.date_vaccination_activity,
  sa.admin1,
  sa.admin2,
  sa.admin3,
  sa.admin4,
  sa.admin5,
  sa.vaccine_administered,
  SUM(sa.indicator_value) filter (where sa.coverage_category in ('Vaccinated')) as total_vaccinated,
  SUM(sa.indicator_value) filter (where sa.coverage_category in ('Deferred')) as total_deferred,
  SUM(sa.indicator_value) filter (where sa.coverage_category in ('Refused')) as total_refused,
  SUM(sa.indicator_value) filter (where sa.coverage_category in ('Deferred Vaccinated')) as total_vaccinated_previously_deferred,
  SUM(sa.indicator_value) filter (where sa.coverage_category in ('Refused Vaccinated')) as total_vaccinated_previously_refused,
  sa.age_group_label,
  sa.latitude,
  sa.longitude
from staging.sia_actuals sa 
group by 1,2,3,4,5,6,7,13,14,15
)
select 
 date,
 a.admin1,
 a.admin2,
 a.admin3,
 a.admin4,
 a.admin5,
 a.vaccine_administered,
 a.total_vaccinated,
 t.campaign_target,
 a.age_group_label,
 a.latitude::real,
 a.longitude::real,
 pic.iso2_code,
 a.total_deferred,
 a.total_refused,
 a.total_vaccinated_previously_deferred,
 a.total_vaccinated_previously_refused
from csv.hard_coded_dates hcd ----has dates listed from 1st December 2022 to 25th August 2025. This file enables one to have a correct way of mapping data across the different time periods
left join actuals a on a.date_vaccination_activity=hcd.date
left join staging.sia_targets  t on a.vaccine_administered=t.vaccine_administered and t.age_group_label=a.age_group_label and a.admin5=t.admin5 and a.date_vaccination_activity between t.campaign_start_date and t.campaign_end_date  
left join csv.province_iso2_codes pic  on pic.province_label=a.admin2
where hcd.date<=now()::date and a.date_vaccination_activity is not null ----Filters dates that are not within the actuals form
);


----This query creates a view for the SIA reasons breakdown
--- This view is useful when creating visuals for the reasons deferred and refused breakdown
--- For the query to execute successfully, the following tables are required:
       --- a. supplemental_immunization_activity_vaccine - the repeat group table within the SIA form
       --- b. supplemental_immunization_activity - the table with the actual vaccination values; vaccinated children, refused children, deferred children
       --- c. admin1 - this table contains all the admin1 level admin names and codes
       --- d. admin2 - this table contains all the admin2 level admin names and codes
       --- e. admin3 - this table contains all the admin3 level admin names and codes
       --- f. admin4 - this table contains all the admin4 level admin names and codes
       --- g. admin5 - this table contains all the admin5 level admin names and codes
create or replace view staging.deferred_refused_reasons as 
(
with deferred_refused_reasons as 
(
select 
  siav.parent_id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  siav.age_group_label,
  unnest(array['Reason 1','Reason 2','Reason 3','Reason 4','Reason 5','Reason 6', 'Reason 7','Reason 8','Reason 9', 'Reason 10','Reason 11','Reason 12','Other','Reason 1',
  'Reason 2','Reason 3','Reason 4','Reason 5','Reason 6','Reason 7','Reason 8','Reason 9','Reason 10','Reason 11','Reason 12','Other']) as reason_category,
  unnest(array[deferred_reason_1,deferred_reason_2,deferred_reason_3,deferred_reason_4,deferred_reason_5,deferred_reason_6,deferred_reason_7,deferred_reason_8,deferred_reason_9,
  deferred_reason_10,deferred_reason_11,deferred_reason_12,deferred_other,refused_reason_1,refused_reason_2,refused_reason_3,refused_reason_4,refused_reason_5,refused_reason_6,
  refused_reason_7,refused_reason_8,refused_reason_9,refused_reason_10,refused_reason_11,refused_reason_12,refused_other]) as reasons_value,
  unnest(array['Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Refused','Refused',
  'Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused']) as coverage_category
from templates.supplemental_immunization_activity_vaccine siav
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id  --- Adds the fields assosciated with the repeat group data
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
)
select * from deferred_refused_reasons
where reasons_value is not null
);


----SIA Records
---This query creates a view that can be used to show the submitted records table
--- For the query to execute successfully, the following tables are required:
       --- a. supplemental_immunization_activity - the table with the actual vaccination values; vaccinated children, refused children, deferred children
       --- b. admin1 - this table contains all the admin1 level admin names and codes
       --- c. admin2 - this table contains all the admin2 level admin names and codes
       --- d. admin3 - this table contains all the admin3 level admin names and codes
       --- e. admin4 - this table contains all the admin4 level admin names and codes
       --- f. admin5 - this table contains all the admin5 level admin names and codes

create or replace view staging.sia_records as 
(
select 
  sia.id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  sia.submitted_at,
  sia.modified_at,
  sia.enumerator 
from templates.supplemental_immunization_activity sia 
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
);


------POWER BI VIEWS
---The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema
---Supplemental Immunization Activity Form
--SIA actual values
create or replace view public.sia_actuals as 
(
select * from staging.sia_actuals
);

---SIA Aggregated values 
create or replace view public.aggregated_sia_actuals_target as 
(
select * from staging.aggregated_sia_actuals_target
);

---SIA Actuals, Target
create or replace view public.sia_actuals_targets as
(
select * from staging.sia_actuals_target sat 
);

----SIA reasons breakdown
create or replace view public.sia_deferred_refused_reasons as 
(
select * from staging.deferred_refused_reasons drr 
);

----SIA Records
create or replace view public.sia_records as 
(
select 
     submission_id,
     date_vaccination_activity,
     admin1,
     admin2,
     admin3,
     admin4,
     admin5,
     vaccine_administered,
     submitted_at::date,
     modified_at::date,
     enumerator
from staging.sia_records
);
