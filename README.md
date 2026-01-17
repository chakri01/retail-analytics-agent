# Retail Insights Assistant – GenAI + Scalable Data System

## Overview
A production-grade GenAI assistant for retail analytics that enables business users to ask natural language questions over sales data and receive accurate, explainable insights.

The system combines strong data engineering foundations with a multi-agent GenAI architecture and strict governance to ensure correctness and prevent hallucinations.  
The solution runs end-to-end on the provided sample retail sales CSV datasets.

---

## 🏗️ System Architecture

The system follows a layered, scalable architecture:

- **Ingestion Layer**: Batch and real-time retail data ingestion  
- **Storage Layer**: Data Lake with Bronze / Silver / Gold tables  
- **Processing Layer**: PySpark + SQL for transformations and aggregations  
- **Analytics Layer**: Governed SQL views and pre-aggregated tables  
- **GenAI Layer**: Multi-agent architecture with validation guardrails  
- **Serving Layer**: Streamlit conversational UI  

📄 **Architecture Presentation (Mandatory)**  
`retail_insights_assistant_architecture.pdf`

Covers:
- System architecture and data flow  
- LLM integration and multi-agent strategy  
- Data storage, indexing, and retrieval design for 100GB+ scale  
- Example query-to-response pipeline  
- Cost, performance, and governance considerations  

---

## 🤖 Multi-Agent GenAI Design

**Agents**
1. **Intent Resolver Agent (LLM)** – Natural language → structured JSON intent  
2. **Data Query Agent (No LLM)** – SQL templates → PostgreSQL execution  
3. **Validation Agent** – Metric, dimension, and result sanity checks  
4. **Insight Narrator Agent (LLM)** – Numbers → business-friendly insights  

**Critical Rule**  
LLMs never access raw tables. All analytics run exclusively on governed SQL views.

---

## 🔍 RAG & Vector Database Strategy

- Vector DB (FAISS) is used for **metadata-only RAG**
- Stores dataset metadata, metric definitions, business glossary, summaries
- Raw transactional retail data is never embedded or retrieved

> RAG guides intent resolution and query planning; analytics remain SQL-driven.

---

## 📈 Scalability Design (100GB+ Retail Data)

- Batch ingestion via PySpark / Databricks Jobs  
- Streaming ingestion via Kafka / Event Hub  
- Delta Lake Bronze / Silver / Gold architecture  
- Pre-aggregated Gold tables for low-latency analytics  
- Cloud warehouse integration (Snowflake / BigQuery / Synapse)  
- Partition pruning, predicate pushdown, cached aggregates  

The GenAI layer remains unchanged as data and compute scale horizontally.

---

## 🛡️ Governance, Security & Monitoring

- SQL templates only (no free-form generation)  
- Central metric dictionary and dataset catalog  
- Validation agent blocks unsupported or ambiguous queries  
- RBAC-based access control  
- No sensitive or raw data exposed to LLMs  
- Query latency, LLM usage, validation failures fully logged  

> The system tracks not only answers given, but also answers intentionally blocked.

---

## 📸 Demo Screenshots & UI Evidence

Screenshots below demonstrate the working chatbot, example Q&A interactions, and automated summary outputs.

- App Home – `images/01_app_home.png`  
- Summary Mode – `images/02_summary_mode.png`  
- Generated Summary – `images/03_generated_summary.png`  
- Conversational Q&A – `images/04_q&a_1.png`, `images/04_q&a_2.png`  
- Logs & Validation – `images/05_logging.png`  

---

## 🚀 Setup & Execution (End-to-End)

Follow the steps below **in order** to run the system on sample retail CSV data.

```bash
# 1. Clone repository
git clone https://github.com/chakri01/retail-analytics-agent.git
cd retail-insights-assistant

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/Scripts/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables (example)
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=retail_analytics
export DB_USER=postgres
export DB_PASSWORD=your_password
export OPENAI_API_KEY=your_openai_api_key

# 5. Ingest sample retail CSV datasets
python run_ingestion.py

# 6. Start multi-agent GenAI backend
python run_agent.py

# 7. Launch Streamlit UI
streamlit run src/ui/streamlit_app.py