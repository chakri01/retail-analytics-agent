"""
Streamlit UI for Retail Insights Assistant - Optimized Summary Mode
"""
import streamlit as st
import pandas as pd
import json
import plotly.express as px
from datetime import datetime
import sys
import os
from io import BytesIO

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
        st.session_state.current_dataset = "auto"
    if 'mode' not in st.session_state:
        st.session_state.mode = "q&a"
    if 'show_details' not in st.session_state:
        st.session_state.show_details = {}
    if 'show_sql' not in st.session_state:
        st.session_state.show_sql = {}
    if 'show_chart' not in st.session_state:
        st.session_state.show_chart = {}
    # Pre-generated summary files (generated once)
    if 'summary_files_generated' not in st.session_state:
        st.session_state.summary_files_generated = False
    if 'summary_files' not in st.session_state:
        st.session_state.summary_files = {}

init_session_state()

# Initialize metadata catalog
metadata_catalog = MetadataCatalog()

# Pre-generate summary files (doesn't make database calls)
def generate_summary_files():
    """Generate summary files from metadata (NO database queries)"""
    try:
        all_views = metadata_catalog.get_all_views()
        
        # 1. JSON Summary
        json_summary = {
            "generated_at": datetime.now().isoformat(),
            "total_views": len(all_views),
            "views": []
        }
        
        for view in all_views:
            view_info = metadata_catalog.get_view_info(view)
            json_summary["views"].append({
                "name": view,
                "description": view_info.get('description', 'No description'),
                "column_count": len(view_info.get('columns', {})),
                "primary_key": view_info.get('primary_key', 'Not specified'),
                "columns": list(view_info.get('columns', {}).keys()),
                "relationships": view_info.get('relationships', [])
            })
        
        # 2. Text Report
        text_report = f"""RETAIL DATASET CATALOG SUMMARY
Generated: {json_summary['generated_at']}
==============================================

OVERVIEW
--------
Total Views: {json_summary['total_views']}
Primary Join Key: sku (all views)

DETAILED VIEW INFORMATION
-------------------------
"""
        
        for view in json_summary["views"]:
            text_report += f"""
View: {view['name']}
Description: {view['description']}
Columns: {view['column_count']}
Primary Key: {view['primary_key']}

Columns: {', '.join(view['columns'][:10])}{'...' if len(view['columns']) > 10 else ''}
{'-' * 50}
"""
        
        text_report += f"""
CROSS-VIEW RELATIONSHIPS
------------------------
All views can be joined using the 'sku' column for comprehensive analysis.

AVAILABLE METRICS
-----------------
• Total sales amount
• Units sold (quantity)
• Order count
• Current stock levels
• Stock status indicators
• Product category distribution
"""
        
        # 3. CSV Data (just view metadata)
        csv_data = []
        for view in json_summary["views"]:
            csv_data.append({
                "view_name": view["name"],
                "description": view["description"],
                "column_count": view["column_count"],
                "primary_key": view["primary_key"],
                "sample_columns": ", ".join(view["columns"][:5])
            })
        csv_df = pd.DataFrame(csv_data)
        
        # Store in session state
        st.session_state.summary_files = {
            "json": json.dumps(json_summary, indent=2),
            "txt": text_report,
            "csv": csv_df.to_csv(index=False)
        }
        st.session_state.summary_files_generated = True
        
        return True
        
    except Exception as e:
        st.error(f"Error generating summary files: {str(e)}")
        return False

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
    
    # Data catalog info
    st.subheader("🗂️ Data Catalog")
    st.info("💡 System automatically detects which datasets to use for your queries.")
    
    # Show available views
    with st.expander("📊 Available Data Views"):
        for view_name in metadata_catalog.get_all_views():
            view_info = metadata_catalog.get_view_info(view_name)
            st.write(f"**{view_name}**: {view_info.get('description', 'No description')}")
    
    # Summary mode controls in sidebar
    if st.session_state.mode == "summary":
        st.subheader("📈 Export Summary")
        
        # One-time generation button
        if not st.session_state.summary_files_generated:
            if st.button("📥 Generate Export Files", type="primary", use_container_width=True):
                with st.spinner("Creating export files..."):
                    if generate_summary_files():
                        st.success("✅ Export files ready!")
                        st.rerun()
        else:
            st.success("✅ Export files ready for download!")
    
    st.markdown("---")
    
    # Clear conversation button
    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.conversation = []
        st.session_state.show_details = {}
        st.session_state.show_sql = {}
        st.session_state.show_chart = {}
        st.rerun()
    
    st.markdown("---")
    
    # System info
    st.caption("🔧 **System Status**")
    st.caption("✅ Database: Connected")
    st.caption("🤖 Agents: Ready")
    st.caption(f"📊 Views: {len(metadata_catalog.get_all_views())}")

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
    user_query = st.chat_input("Ask any business question about sales, inventory, or products...", key="query_input")
    
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
                result = st.session_state.orchestrator.process_query(user_query)
            
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
    st.markdown("### 📊 Dataset Catalog Summary")
    
    if st.session_state.summary_files_generated:
        # Show available exports
        st.success("✅ Export files are ready for download!")
        
        # Export options
        st.subheader("📤 Download Summary Files")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # JSON Export
            st.download_button(
                label="💾 Download JSON",
                data=st.session_state.summary_files["json"],
                file_name=f"dataset_catalog_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json",
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            # Text Report Export
            st.download_button(
                label="📝 Download Text Report",
                data=st.session_state.summary_files["txt"],
                file_name=f"dataset_catalog_{datetime.now().strftime('%Y%m%d')}.txt",
                mime="text/plain",
                use_container_width=True,
                type="primary"
            )
        
        with col3:
            # CSV Export
            st.download_button(
                label="📊 Download CSV",
                data=st.session_state.summary_files["csv"],
                file_name=f"dataset_catalog_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary"
            )
        
        # Preview of what's included
        st.markdown("---")
        st.subheader("📋 Preview of Included Data")
        
        # Show quick stats
        try:
            summary_data = json.loads(st.session_state.summary_files["json"])
            
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            with stats_col1:
                st.metric("Total Views", summary_data["total_views"])
            with stats_col2:
                total_columns = sum(v["column_count"] for v in summary_data["views"])
                st.metric("Total Columns", total_columns)
            with stats_col3:
                st.metric("Generated", datetime.fromisoformat(summary_data["generated_at"]).strftime("%H:%M"))
            
            # Show first 3 views as preview
            st.markdown("**Sample Views:**")
            for view in summary_data["views"][:3]:
                with st.expander(f"**{view['name']}** - {view['description']}"):
                    st.markdown(f"**Columns:** {view['column_count']}")
                    st.markdown(f"**Primary Key:** `{view['primary_key']}`")
                    st.markdown(f"**Sample Columns:** {', '.join(view['columns'][:5])}")
            
            if len(summary_data["views"]) > 3:
                st.info(f"... and {len(summary_data['views']) - 3} more views")
                
        except:
            st.info("Click the download buttons to get the complete dataset catalog.")
        
    else:
        # Initial state - prompt to generate summary
        st.info("👈 Click 'Generate Export Files' in the sidebar to create summary files")
        
        # Show what will be included
        st.markdown("### 📋 What will be included:")
        
        preview_col1, preview_col2 = st.columns(2)
        
        with preview_col1:
            st.markdown("**JSON Export:**")
            st.markdown("• Complete metadata structure")
            st.markdown("• View descriptions and schemas")
            st.markdown("• Column lists and counts")
            st.markdown("• Primary keys and relationships")
        
        with preview_col2:
            st.markdown("**Text & CSV Exports:**")
            st.markdown("• Formatted text report")
            st.markdown("• CSV with view metadata")
            st.markdown("• Ready for documentation")
            st.markdown("• No database queries needed")
        
        # Show available views
        st.markdown("### 📊 Available Views:")
        all_views = metadata_catalog.get_all_views()
        for view_name in all_views[:5]:  # Show first 5
            view_info = metadata_catalog.get_view_info(view_name)
            st.markdown(f"**{view_name}**: {view_info.get('description', 'No description')}")
        
        if len(all_views) > 5:
            st.info(f"... and {len(all_views) - 5} more views")

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.caption("🤖 Powered by CrewAI + OpenAI")
with col2:
    st.caption("📊 Data from PostgreSQL views")
with col3:
    st.caption("🔒 LLMs never touch raw tables")