"""
Logging configuration for Phase 3
"""
import logging
import json
from datetime import datetime
from pathlib import Path

def setup_phase3_logging():
    """Configure logging for Phase 3"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create loggers for each agent
    loggers = {}
    
    agents = ["intent_resolver", "data_query", "validation", "narrator", "orchestrator"]
    
    for agent in agents:
        # Create logger
        logger = logging.getLogger(f"retail_insights.{agent}")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # Create file handler
        log_file = log_dir / f"phase3_{agent}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        loggers[agent] = logger
    
    # Also configure root logger
    root_logger = logging.getLogger("retail_insights")
    root_logger.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    return loggers

class agentLogger:
    """Logger for Phase 3 activities"""
    
    def __init__(self):
        self.loggers = setup_phase3_logging()
    
    def log_intent(self, user_query: str, intent: dict, agent: str = "intent_resolver"):
        """Log intent resolution"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "query": user_query,
            "intent": intent,
            "agent": agent
        }
        self.loggers.get(agent, self.loggers["orchestrator"]).info(
            f"Intent resolved: {json.dumps(log_entry)}"
        )
    
    def log_sql(self, sql: str, intent: dict = None):
        """Log generated SQL"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "sql": sql,
            "intent": intent or {}
        }
        self.loggers["data_query"].info(
            f"SQL generated: {json.dumps(log_entry)}"
        )
    
    def log_validation(self, validation_type: str, result: dict):
        """Log validation result"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": validation_type,
            "result": result
        }
        self.loggers["validation"].info(
            f"Validation {validation_type}: {json.dumps(log_entry)}"
        )
    
    def log_results(self, row_count: int, columns: list, query_time: float = None):
        """Log query results"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "row_count": row_count,
            "columns": columns,
            "query_time_ms": query_time
        }
        self.loggers["data_query"].info(
            f"Query results: {json.dumps(log_entry)}"
        )
    
    def log_agent_decision(self, agent: str, decision: str, reason: str = "", confidence: float = None):
        """Log agent decision"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "agent": agent,
            "decision": decision,
            "reason": reason,
            "confidence": confidence
        }
        self.loggers[agent].info(
            f"Agent decision: {json.dumps(log_entry)}"
        )