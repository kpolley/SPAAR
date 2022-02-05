# SPAAR
### Spark Platform for Alerting and Automated Response

<p align="center">
  <img src="assets/logo.png" />
</p>

## Goals
### Infrastructure
* **Scalable**: ingest, alert, and analyze logs at any scale regardless of size. SPAAR will handle autoscaling pods during spikes
* **Simple**: the only infrastructure you need is a Spark cluster and a filesystem such as S3 or Hadoop. 
* **Standardized**: Use common tools and formats such as Python and Parquet
* **Inexpensive**:  Only pay for compute and storage cost

### Alerting & Automated Response
* **Detection as Code**: Python enables the team to build flexible detections, modularize and re-use logic, and use well-known and supported libraries such as `geoip2` for geoip lookup or even `scikit-learn` for machine learning.
* **Version Control**: Revert to pervious states swiftly and provide needed context for detection changes
* **Unit Tests**: A test-driven approach to detection and response engineering ensures quality and inspires confidence. 

## Prerequisites
* A spark cluster or single-node deployment
* AWS access keys with permission to read/write to appropriate S3 buckets, and optionally SNS in order to send alerts