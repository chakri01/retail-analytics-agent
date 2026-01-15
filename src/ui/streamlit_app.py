"""
Streamlit UI for Retail Insights Assistant - Fixed version
"""
import streamlit as st
import pandas as pd
import json
import plotly.express as px
from datetime import datetime
import sys
import os

# Fix import paths - Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

# Now import from src
from src.agents.crew_orchestrator import CrewOrchestrator
from src.vector_db.metadata_catalog import MetadataCatalog

# Page configuration
st.set_page_config(
    page_title="Retail Insights Assistant",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #374151;
        margin-bottom: 1rem;
    }
    .data-card {
        background-color: #F3F4F6;
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    .insight-box {
        background-color: #E0F2FE;
        border-left: 5px solid #0369A1;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 5px;
    }
    .metric-card {
        background-color: white;
        border-radius: 10px;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
def init_session_state():
    if 'orchestrator' not in st.session_state:
        st.session_state.orchestrator = CrewOrchestrator()
    if 'conversation' not in st.session_state:
        st.session_state.conversation = []
    if 'current_dataset' not in st.session_state:
        st.session_state.current_dataset = "amazon_sales"
    if 'mode' not in st.session_state:
        st.session_state.mode = "q&a"
    if 'summary_data' not in st.session_state:
        st.session_state.summary_data = None
    if 'show_details' not in st.session_state:
        st.session_state.show_details = {}
    if 'show_sql' not in st.session_state:
        st.session_state.show_sql = {}
    if 'show_chart' not in st.session_state:
        st.session_state.show_chart = {}

init_session_state()

# Initialize metadata catalog
metadata_catalog = MetadataCatalog()

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/analytics.png", width=80)
    st.title("Retail Insights Assistant")
    
    st.markdown("---")
    
    # Mode selector
    st.subheader("📋 Mode Selection")
    mode = st.radio(
        "Choose interaction mode:",
        ["💬 Q&A Chat", "📊 Dataset Summary"],
        index=0 if st.session_state.mode == "q&a" else 1,
        key="mode_selector",
        label_visibility="collapsed"
    )
    
    st.session_state.mode = "q&a" if mode == "💬 Q&A Chat" else "summary"
    
    st.markdown("---")
    
    # Dataset selector
    st.subheader("🗂️ Dataset")
    datasets = metadata_catalog.get_all_datasets()
    selected_dataset = st.selectbox(
        "Select dataset:",
        datasets,
        index=datasets.index(st.session_state.current_dataset) if st.session_state.current_dataset in datasets else 0,
        help="Choose which dataset to query"
    )
    
    st.session_state.current_dataset = selected_dataset
    
    # Show dataset info
    dataset_info = metadata_catalog.get_dataset_info(selected_dataset)
    with st.expander("📝 Dataset Info", expanded=True):
        st.caption(f"**Description**: {dataset_info.get('description', 'No description')}")
        
        metrics = dataset_info.get('metrics', [])
        if metrics:
            st.caption("**Metrics**:")
            for metric in metrics[:3]:  # Show first 3
                st.caption(f"  • {metric['name']}: {metric['description']}")
            if len(metrics) > 3:
                st.caption(f"  • ... and {len(metrics) - 3} more")
        
        dimensions = dataset_info.get('dimensions', [])
        if dimensions:
            st.caption("**Dimensions**:")
            for dim in dimensions[:3]:  # Show first 3
                st.caption(f"  • {dim['name']}: {dim['description']}")
            if len(dimensions) > 3:
                st.caption(f"  • ... and {len(dimensions) - 3} more")
    
    st.markdown("---")
    
    # Summary mode controls
    if st.session_state.mode == "summary":
        st.subheader("📈 Summary Options")
        if st.button("🔄 Generate Summary", use_container_width=True):
            with st.spinner("Generating summary..."):
                st.session_state.summary_data = st.session_state.orchestrator.get_summary(selected_dataset)
    
    st.markdown("---")
    
    # Clear conversation button
    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.conversation = []
        st.session_state.summary_data = None
        st.session_state.show_details = {}
        st.session_state.show_sql = {}
        st.session_state.show_chart = {}
        st.rerun()
    
    st.markdown("---")
    
    # System info
    st.caption("🔧 **System Status**")
    st.caption("✅ Database: Connected")
    st.caption("🤖 Agents: Ready")
    st.caption(f"📊 Data: {selected_dataset} selected")

# Main content area
st.markdown('<h1 class="main-header">📊 Retail Insights Assistant</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Ask questions about your retail data in natural language</p>', unsafe_allow_html=True)

# Q&A Mode
if st.session_state.mode == "q&a":
    
    # Display conversation history
    if st.session_state.conversation:
        st.markdown("### 💬 Conversation History")
        for i, message in enumerate(st.session_state.conversation[-5:]):  # Show last 5 messages
            with st.chat_message(message["role"]):
                st.write(message["content"])
                
                if message["role"] == "assistant" and "data" in message:
                    message_id = hash(json.dumps(message, sort_keys=True))
                    
                    # Show data button
                    if st.button(f"📋 View Data for Query {i+1}", key=f"view_data_{message_id}"):
                        st.session_state.show_details[message_id] = not st.session_state.show_details.get(message_id, False)
                    
                    # Show data if toggled
                    if st.session_state.show_details.get(message_id, False) and message.get("data"):
                        df = pd.DataFrame(message["data"])
                        st.dataframe(df, use_container_width=True)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Rows", message.get("row_count", 0))
                        with col2:
                            st.metric("Columns", len(message.get("columns", [])))
                        
                        # Show SQL button
                        if st.button(f"🔍 View SQL for Query {i+1}", key=f"view_sql_{message_id}"):
                            st.session_state.show_sql[message_id] = not st.session_state.show_sql.get(message_id, False)
                        
                        if st.session_state.show_sql.get(message_id, False) and message.get("sql"):
                            st.code(message["sql"], language="sql")
    
    # Chat input
    user_query = st.chat_input(f"Ask about {st.session_state.current_dataset}...")
    
    if user_query:
        # Add user message to conversation
        st.session_state.conversation.append({
            "role": "user",
            "content": user_query
        })
        
        # Display user message immediately
        with st.chat_message("user"):
            st.write(user_query)
        
        # Process query
        with st.chat_message("assistant"):
            with st.spinner("🔍 Analyzing intent..."):
                result = st.session_state.orchestrator.process_query(
                    user_query, 
                    st.session_state.current_dataset
                )
            
            if result["success"]:
                # Display narration
                st.write(result["narration"])
                
                # Display data if available
                if result.get("data"):
                    message_id = hash(json.dumps(result, sort_keys=True))
                    
                    # Create columns for buttons
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("📊 View Detailed Data", key=f"details_{message_id}"):
                            st.session_state.show_details[message_id] = not st.session_state.show_details.get(message_id, False)
                    
                    with col2:
                        if st.button("🔍 View SQL", key=f"sql_{message_id}"):
                            st.session_state.show_sql[message_id] = not st.session_state.show_sql.get(message_id, False)
                    
                    with col3:
                        if st.button("📈 Visualize", key=f"chart_{message_id}"):
                            st.session_state.show_chart[message_id] = not st.session_state.show_chart.get(message_id, False)
                    
                    # Show detailed data if toggled
                    if st.session_state.show_details.get(message_id, False):
                        df = pd.DataFrame(result["data"])
                        st.dataframe(df, use_container_width=True)
                        
                        # Basic metrics in columns
                        mcol1, mcol2, mcol3 = st.columns(3)
                        with mcol1:
                            st.metric("Records", result.get("row_count", 0))
                        with mcol2:
                            st.metric("Columns", len(result.get("columns", [])))
                        with mcol3:
                            dataset = result.get("intent", {}).get("dataset", "N/A")
                            st.metric("Dataset", dataset)
                        
                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name=f"retail_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            key=f"download_{message_id}"
                        )
                    
                    # Show SQL if toggled
                    if st.session_state.show_sql.get(message_id, False) and result.get("sql"):
                        st.markdown("### Generated SQL Query")
                        st.code(result.get("sql", ""), language="sql")
                    
                    # Show chart if toggled
                    if st.session_state.show_chart.get(message_id, False) and result.get("data"):
                        df = pd.DataFrame(result["data"])
                        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                        
                        if numeric_cols and len(df) > 1:
                            st.markdown("### 📈 Data Visualization")
                            
                            # Simple chart selection
                            if len(df) <= 20:  # Only show bar chart for small datasets
                                x_col = st.selectbox("X-axis", df.columns, key=f"x_{message_id}")
                                y_col = st.selectbox("Y-axis", numeric_cols, key=f"y_{message_id}")
                                
                                if x_col and y_col:
                                    fig = px.bar(df, x=x_col, y=y_col, 
                                                 title=f"{y_col} by {x_col}")
                                    st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("Chart visualization works best with datasets under 20 rows.")
                
                # Add assistant message to conversation
                st.session_state.conversation.append({
                    "role": "assistant",
                    "content": result["narration"],
                    "data": result.get("data"),
                    "row_count": result.get("row_count"),
                    "columns": result.get("columns"),
                    "sql": result.get("sql")
                })
                
            else:
                # Handle error
                error_msg = result.get("error", "Unknown error")
                
                if result.get("should_clarify"):
                    clarification_msg = f"I need clarification: {error_msg}. Could you rephrase your question?"
                else:
                    clarification_msg = f"I encountered an error: {error_msg}. Please try again or ask a different question."
                
                st.error(clarification_msg)
                
                # Add error to conversation
                st.session_state.conversation.append({
                    "role": "assistant",
                    "content": clarification_msg
                })

# Summary Mode
else:
    st.markdown("### 📊 Dataset Summary")
    
    if st.session_state.summary_data:
        summary = st.session_state.summary_data
        
        st.success(f"Generated summary for **{summary['dataset']}** with {summary['total_queries']} insights")
        
        for i, item in enumerate(summary["summary_items"], 1):
            with st.container():
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"**{i}. {item['query']}**")
                with col2:
                    st.metric("Rows", item.get("row_count", 0))
                
                st.markdown(f'<div class="insight-box">{item["narration"]}</div>', unsafe_allow_html=True)
                st.markdown("---")
    else:
        st.info("👈 Select a dataset and click 'Generate Summary' in the sidebar to get started.")
        
        # Show sample questions
        st.markdown("### 💡 Try these questions in Q&A mode:")
        
        sample_questions = [
            "What are total sales by category?",
            "Which country has the highest sales?",
            "Show me top 10 products by sales",
            "What is the monthly sales trend?",
            "How much inventory is low stock?"
        ]
        
        for question in sample_questions:
            if st.button(f"❓ {question}", key=f"sample_{question}", use_container_width=True):
                st.session_state.mode = "q&a"
                st.session_state.conversation = []  # Clear conversation
                st.rerun()

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.caption("🤖 Powered by CrewAI + OpenAI")
with col2:
    st.caption("📊 Data from PostgreSQL views")
with col3:
    st.caption("🔒 LLMs never touch raw tables")