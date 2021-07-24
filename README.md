# Pinecone

Pinecone is a Data Reconciliation application build on Scala to reconciling data between multiple databases with various reporting and monitoring capabilities. It comes up with a configurable approach and different deployment strategies fit for many data teams to operating reconciliation workloads efficiently.

# Motivation

Data mismatch has always been a headache problem for data engineer teams who maintain multiple data pineline workloads sourcing from several databases ranging from back-end application team to ETL pipeline from persistent stage to Data mart. Data stakeholders come to the team and say there is a data mismatch or at least their suspectation. The team needs to manually run SQL queries against data source, and then data destination. Sometimes, it is a historical data which has nothing wrong in the past but suddenly mismatch, or you just run a job, finish the pineline and want to make sure that data looks OK. These kinds of works happen all the time and eventually become a "new official" job description since the datasets growing gradually.

Pinecone aims to solve the problem by bringing a capability to form up SQL queries run against source and target data. It provides a set of rules for users to set up their expectation on accepted deviation and how do you want to compare the results. It also provides `Pinecone variable` which is the variable template that you can use in your own SQL queries and it will be transformed to the value at run-time. Finally, set your own rules by defining how to monitor it and how to make changes with ease, integrating with modern orchestration workflow such as Airflow to operate in data engineering fashion.
 

# Note

This project developed internally in my company. I'm rewrite it in generall approach to release as an open source project.
To be release soon.
All contribution more than welcome.
Please reach out me on email strider.giang@live.com or Github issues.