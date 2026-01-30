import os
from pathlib import Path

class PipelineConfig:
    
    @staticmethod
    def get_db_path() -> str:
        """
        Get SQLite database path with fallback:
        1. POKE_DB_PATH env var (for custom locations)
        2. Project-relative path (for portability)
        """
        # Check env var first
        env_path = os.getenv('POKE_DB_PATH')
        if env_path:
            return env_path
        
        # Fallback to project structure
        project_root = Path(__file__).parent.parent.parent.parent
        return str(project_root / "include" / "poke_api" / "db" / "poke_db.db")