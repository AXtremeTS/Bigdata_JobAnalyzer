# scripts/run-pipeline.py
"""
Wrapper pointing to 2_pipeline/run_pipeline.py for backward compatibility.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_RUNNER = os.path.join(PROJECT_ROOT, "2_pipeline", "run_pipeline.py")

if __name__ == "__main__":
    if os.path.exists(PIPELINE_RUNNER):
        import subprocess
        sys.exit(subprocess.call([sys.executable, PIPELINE_RUNNER]))
    else:
        print(f"Error: Could not find {PIPELINE_RUNNER}")
        sys.exit(1)
