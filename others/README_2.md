# Phase 3: GenAI + Agents + UI

## 🎯 Phase 3 Objective
Convert the solid data system from Phase 2 into a working GenAI product with a multi-agent architecture and Streamlit UI.

## 🏗️ Architecture

### 4-Agent System
1. **Intent Resolver Agent** (Gemini) - Converts natural language → structured JSON intent
2. **Data Query Agent** (NO LLM) - Executes SQL templates against PostgreSQL views
3. **Validation Agent** - Governance and anti-hallucination firewall
4. **Insight Narrator Agent** (Gemini) - Converts numbers → business-friendly text

### 🚫 Non-negotiable Rule
**LLMs never touch raw tables directly.** All data access goes through:
- `sales_fact_view`
- `product_dim_view` 
- `inventory_dim_view`

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt