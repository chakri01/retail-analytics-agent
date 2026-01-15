"""
Fix OpenAI dependencies
"""
import subprocess
import sys

def fix_dependencies():
    print("Fixing OpenAI dependencies...")
    print("="*60)
    
    # Uninstall problematic packages
    print("1. Uninstalling old packages...")
    packages_to_remove = ["openai", "openai-client"]
    for package in packages_to_remove:
        subprocess.call([sys.executable, "-m", "pip", "uninstall", "-y", package])
    
    # Install correct versions
    print("\n2. Installing correct versions...")
    requirements = [
        "openai>=1.13.3,<2.0.0",  # CrewAI compatible version
        "crewai==0.28.0",
        "streamlit==1.28.0",
        "faiss-cpu==1.7.4",
        "sentence-transformers==2.2.2",
        "langchain==0.1.16",
        "plotly==5.18.0"
    ]
    
    for req in requirements:
        print(f"   Installing {req}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", req])
        except:
            print(f"   Warning: Failed to install {req}")
    
    print("\n" + "="*60)
    print("✅ Dependencies fixed!")
    print("\nNow test with: python test_openai_fixed.py")

if __name__ == "__main__":
    fix_dependencies()