# RT-VaMA
Development of data tools for Real-time Vaccination Monitoring and Analysis Deployment Toolkit (RT-VaMA)

The RT-Vama project has developed 6 form templates and uploaded them to the RT-VaMA project on Inform. These forms should cover all the steps in a vaccination campaign, from preparation, to monitoring, to communication activities. Except for form 2 and 3, which represent targets and actuals of the campaign, the forms are independent from one another, meaning that each country adopting the toolkit is able to pick only the relevant forms based on the local priorities. 

The form templates include:

1. [Health Center Level Monitoring And Assessment of Readiness](https://docs.google.com/spreadsheets/d/1OxO4w6VWKYusa2OvKE2YJdzgCaD6KtUGDta1pDFqZKA/edit#gid=0) - data is captured before the campaign starts to assess the readiness of the health center.
2. [Supplemental Immunization Activity Targets](https://docs.google.com/spreadsheets/d/1pz_fTkwNVtyJKR5opKOUEFtMaUKbceTdhb2ustBF1Og/edit#gid=0) - captures the campaign targets at either health center or administrative level before the health center.The data captured includes; location hierarchies, age group, vaccine to be administered, no. of children to be vaccinated, received vaccine vials.
3. [Supplemental Immunization Activity](https://docs.google.com/spreadsheets/d/1nf6MieEE8eTnhfK5479qrtkjpAmOJ1NF/edit#gid=88380113) - captures the actual values for the different campaign days. The data captured includes; type of vaccine administered,age group,gender, no. of vaccinated children, no.of deferrals, no. of refusals, vials used.
4. [SPV Rapid Coverage Assessment](https://docs.google.com/spreadsheets/d/1SbFKz2o_fFTBStcAFKd_BsMswxyk6KCondzGk_MI72Q/edit#gid=0) - done after the vaccination campaign to assess coverage. The data captured includes; immunization coverage, unvaccinated reasons and sources of information.
5. [Synchronized Vaccination Monitoring Tool](https://docs.google.com/spreadsheets/d/1cKpTgs_zCiyt21JQPOHrVBlfHiA8-8ikoE-nS_RVnhM/edit#gid=0) - done during the campaign and its usually random no specified time. Hence, data could change everyday based on the recommendation given. Consists of yes/no questions on vaccine management, vaccination sites and vaccination teams and the microplan check.
6. [SPV Social Mobilization Indicators](https://docs.google.com/spreadsheets/d/1nv99rrBvXO_Bw5GqUBYEk5ycOIstp8hw2NHXwXuwrqs/edit#gid=0) - done during or after the vaccination campaign. Mainly used for communication activities. The data captured includes; no.of religious institutions visited, no. of advocacy meetings held, no. of social mobilisers engaged, no. of doors visited, no. of refusals addressed and no. of posters and banners displayed.

The objective of these data transformation scripts is to extract the data from these forms and create tables that can be used for reporting. Given the independence of the various forms, the toolkit is subdivided into 6 scripts, so that only the relevant scripts are required based on forms adopted by the campaign. 

The data transformation scripts include: 

1. hcl_readiness_script. This script transforms data from the `Health Center Level Monitoring and Assessment of Readiness` form. Within this script there are 2 SQL queries, one that unnests the forms questions and another one that calculates the readiness score based on the question categories that are available.

2. sia_script. This script transforms data from the `Supplemental Immunization Activity` and `Supplemental Immunization Activity Targets` form. Within the script, there are 6 SQL queries one that pulls the choice labels from the labels table, one that retrieves the actual values from the SIA form, one that retrieves the actual and target values from the SIA and SIA targets form, one that retrieves the deferred and refused reasons breakdown from the SIA form and one that calculates the cumulative values of the vials used and discarded, vaccinated children and campaign target.

3. rca_script. This script transforms data from the `Rapid Coverage Assessment` form. Within the script there are 5 SQL queries, one that creates a labels table for the RCA form, one that retrieves the actual no. of children who are present in the hh, have been vaccinated, not been vaccinated, one that retrieves not vaccinated reasons breakdown, one that 
retrieves the sources of information from the RCA form.

4. social_mobilization_indicators_script. This script transforms data from the `Social Mobilization Indicators` form. Within the script there is one SQL query that retrieves the social mobilization activities from the form.

5. synchronized_monitoring_tool_script. This script transforms data from the `Synchronized Vaccination Monitoring Tool` form. Within the script there are 3 SQL queries,one that unnests the form questions, one that calculates the monitoring score of the facility based on the question categories and one that retrieves the number of children who have been vaccinated and target values from the form.

Each script works to create a set of final tables that can be connected to the BI tool of choice (e.g. PowerBI, Superset). They achieve the objective according to the same logic:
- Create labels if any
- Create intermediate table / views
- Create final tables with logic for metrics

In order to edit these scripts, a country office should: 
1. Know the tables associated with each script.
2. Select the relevant scripts.
3. Understand SQL

It is highly recommended to be careful while customizing the scripts so as to avoid the queries from not being executed successfully.

Please note that, in order to execute the queries, you will need access to the form submissions, as well as the administrative areas tables and in some cases, the table with the listed dates and province iso codes.
