# RT-VaMA
Development of data tools for Real-time Vaccination Monitoring and Analysis Deployment Toolkit (RT-VaMA)

The RT-Vama project has developed 6 form templates and uploaded them to the RT-VaMA project on Inform. These forms should cover all the steps in a vaccination campaing, from preparation, to monioting, to communication activities. Except for form 2 and 3, which represent targets and actuals of the campaign, the forms are independent from one another, meaning that each country adopting the toolkit is able to pick only the relevant forms based on the local priorities. 

The form templates include:

1. [Health Center Level Monitoring And Assessment of Readiness](https://docs.google.com/spreadsheets/d/1OxO4w6VWKYusa2OvKE2YJdzgCaD6KtUGDta1pDFqZKA/edit#gid=0) - data is captured before the campaign starts to assess the readiness of the health center.
2. [Supplemental Immunization Activity Targets](https://docs.google.com/spreadsheets/d/1pz_fTkwNVtyJKR5opKOUEFtMaUKbceTdhb2ustBF1Og/edit#gid=0) - captures the campaign targets at either health center or administrative level before the health center.The data captured includes; location hierarchies, age group, vaccine to be administered, no. of children to be vaccinated, received vaccine vials.
3. [Supplemental Immunization Activity](https://docs.google.com/spreadsheets/d/1nf6MieEE8eTnhfK5479qrtkjpAmOJ1NF/edit#gid=88380113) - captures the actual values for the different campaign days. The data captured includes; type of vaccine administered,age group,gender, no. of vaccinated children, no.of deferrals, no. of refusals, vials used.
4. [SPV Rapid Coverage Assessment](https://docs.google.com/spreadsheets/d/1SbFKz2o_fFTBStcAFKd_BsMswxyk6KCondzGk_MI72Q/edit#gid=0) - done after the vaccination campaign to assess coverage. The data captured includes; immunization coverage, unvaccinated reasons and sources of information.
5. [Synchronized Vaccination Monitoring Tool](https://docs.google.com/spreadsheets/d/1cKpTgs_zCiyt21JQPOHrVBlfHiA8-8ikoE-nS_RVnhM/edit#gid=0) - done during the campaign and its usually random no specified time. Hence, data could change everyday based on the recommendation given. Consists of yes/no questions on vaccine management, vaccination sites and vaccination teams and the microplan check.
6. [SPV Social Mobilization Indicators](https://docs.google.com/spreadsheets/d/1nv99rrBvXO_Bw5GqUBYEk5ycOIstp8hw2NHXwXuwrqs/edit#gid=0) - done during or after the vaccination campaign. Mainly used for communication activities. The data captured includes; no.of religious institutions visited, no. of advocacy meetings held, no. of social mobilisers engaged, no. of doors visited, no. of refusals addressed and no. of posters and banners displayed.

The objective of these data transformation scripts is to extract the data from these forms and create tables that can be used for reporting. Given the independence of the various forms, the toolkit is subdivided into XXX scripts, so that only the relevant scripts are required based on forms adopted by the campaing. 

The data transformation scripts include: 

1. hcl_readiness_script. This script transforms data from the `Health Center Level Monitoring and Assessment of Readiness` form. Within this script there are 2 SQL queries, one that unnests the forms questions and another one that calculates the readiness proportion based on the question categories that are available.


3. rca_script.sql This script transforms data from the `Rapid Convenience Assessment` form. Please note that, in order to execute the queries, you'll need access to the form submissions, as well as to the administrative areas and the household list attached to the original form (`spv_rapid_coverage_assessment_form_vaccination`) 

-- To be able to execute the queries below, you'll need access to the following tables 
