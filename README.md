
# 🏡 Virtual Realtor – Data Pipeline

This repository is part of the **Virtual Realtor Application**, an AI-powered system designed to help users explore real estate properties, ask questions, and receive intelligent recommendations.  

The `hpd_data_pipeline` module contains the **data engineering pipeline**, ensuring smooth extraction, transformation, embedding generation, and storage of property data for use in the application.

---

## 🚀 Overview

The  Virtual Realtor: data engineering pipeline combines:
- **Real estate property data ingestion**  
- **Data preprocessing & cleaning**  
- **Vector embedding generation** (for semantic search & Q&A)  


This repo (`hpd_data_pipeline`) focuses on the **data layer**, preparing the dataset for downstream AI/ML components.

## 🗄️ Data 

| Component        | Technology   | Purpose                                          |
|------------------|--------------|--------------------------------------------------|
| Data Platform    | Databricks   | Data engineering, ETL pipelines, preprocessing   |
| Vector Database  | Pinecone     | Storing & retrieving embeddings for semantic search |


---


# DataFlow

<img width="1742" height="1988" alt="diagram-export-9-21-2025-11_48_41-AM" src="https://github.com/user-attachments/assets/9737f5ca-1edd-4859-8f73-fa69d2fa1fa4" />
