# Discussion

I was not able to find a way to link any of the data together given this small subset of data.

I would imagine for larger datasets, I would be able to identify how to join some of these data sets together.

My approach for this problem given a short time investment was to quickly map each table to the output schema and union them together.

Downside of this is not being able to connect any of the salesforce data with the leads. More thoughtful approaches and more data is needed to formulate how to intelligently integrate the lead data with the rest of the data sources

With more time, there are a lot of things we can do. Many of the fields are messy and need some type of cleaning. Some sources are missing city, county for example, but this can be derived from address. Sometimes address1 + address2 is combined into one column. A lot of this can be cleaned up if we use a python script to input the data into a geographic API like Census Bureau of Geocoding API to fetch the missing geographic attributes input based on street address and separate them into various attributes we should be able to access. Another approach is to identify common identifiers of address2 like apt, suite, ste and do a regex extraction based on those to compile a more accurate address. The advantage of cleaning up address is being able to determine good territories for lead conversions from a geographic perspective and potentially joining the data together via full address of a business when joining by the name is not feasible.

The name/company in general is very messy and a lot of cleaning can be done here to extract useful information. Some of the cleaning may require LLMs especially to quickly determine if the entity is a person or a company.

I was able to infer some of the columns based on extracting from columns, but there were additional ways to enrich the data I did not explore also due to the test nature of the data not having real phone numbers, emails on the lead data. But I would imagine we would be able to determine good/bad lead quality by correlating/joining the lead data with the other sources to determine help determine levers that may indicate high lead quality. We would be able to identify things like, what type of curriculum is correlated with high quality leads, what cities or geographic areas?

With a larger dataset, additional cleaning, and entity resolution on names and address data, we can link people related data across all the sources to build a better picture of the relations between the datasets.

Additionally with more time and larger data scope, I would create better data models rather than the current state. Ideally, we should definitely start with a data vault style architecture with hubs, links, and satellites. The main reason for choosing this model over say kimball or inmon is because of the variety of different sources of data with different schemas with potential frequent changes of schema of existing sources. With kimball or inmon handling schema changes would create a lot of overhead whereas data vault can add new hubs, links, and sattelites without impacting existing models. Data vault also enables historical tracking which is critical for lead tracking. Potential downside is needing to invest more effort create a single source of truth. Once the data vault model has finished processing, the next step would be to create a single source of truth with 3NF style data that runs after the data vault stage to establish a clean enterprise data warehouse. So this could include having a schools table, licenses table, contacts table, programs table, leads table, people table with bridge tables established for many to many relationships. Below this, we would create star schema styled tables that optimize downstream analytics and answering questions like how many leads were converted by each school in the past month? What is the average response time by lead status for different school types? This information can be part of a report of an automation process via Python/SQL on demand or for more flexible interactive viewing and more like a tool for sales team is to be inserted into a BI tool for more structured lead information.

Given that the file load will include a full refresh of existing and net new records, we need an intelligent way to manage this rather than having to do full refresh every time. One approach is to introduce a staging phase before the data is loaded as a new raw data in BQ replacing the raw models to check for schema differences between the incoming data and the existing schema. We can use dbt configs to do full refresh only when there is a schema difference, otherwise we can use an incremental materialized dbt model on some unique key based on the data source to minimize run time. For example, the leads data could be lead_id. That would be a potential solution if the existing lead data is not expected to change without schema changes, but if the data themselves can also join, additional work arounds may need to be developed around that to minimize redundant data processing.

Long term, we will also need to set up data orchestration via airflow or dagster (preferred) with dbt so that the data pipelines are able to kick off automatically on a sensor cadence based on new files detected in the data lake (Google storage or s3).

Once we understand the nuances around the data and what bad and good quality is around each column of data, we need to write dbt unit tests for each data modeling process to ensure that those assumptions are holding true for future data coming in. Based on the check, it can be a warn or fail depending on the use case and consequence of violating said assumption.

One other thing I noticed with the data files is not having information on whether a license is active or not, but there is an expiration date. Ideally, we would need to create a time series to keep track of the "current" status of each person so we know at what point they were active or not. This would have to be done using a SCD type 2 or type 4 depending on downstream analytical use cases of this dimension assuming that it's normal for it to go between inactive or not. 

We have some potential use cases using external datasets to enrich the data, mostly on a geographical and demographic standpoint. We might be able to calculate the distance to similar schools/curriculum types. What areas are less or more saturated and correlate with leads. Add general information like age distributions, education, household income based on zip codes, states, cities, phone area code, etc. It could be good to target geographic areas with left tail heavy age population. Is there anywhere we can source license data about who is licensed, up to date, etc. Google reviews for each school location and leverage the reviews, ratings, what people are saying, social media. Web scraping the website for the school for any additional information. Any vendor data that we can purchase to have a more of an insider story for each school like their revenue, employee retention, etc. What about crime rates for each geographic area? Can we track how often the school's website are visited and what times of day are they most active, how are engagement rates, etc. How do competitor pricing plans look compared to a school? (web scraping)


