# gnomAD-mutation-query
The goal of this project is to demostrate the low cost of BigQuery to run on entire chromosomes. This repository provides a framework to programmatically interact with BigQuery.

### Getting Started
```
#Follow Before you begin steps: https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries

#Clone the repo
git clone https://github.com/tsa87/gnomAD-mutation-query.git

#Install the Python Client library
pip install --upgrade google-cloud-bigquery
```

We generated plot for mutation count (<= 1%) using Chr1 with 248900000 positions processing only 1.43 GB, well below 1TB free-tier limit offered by BigQuery
