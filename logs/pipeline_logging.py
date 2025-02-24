from loguru import logger
from datetime import datetime
import os

class PipelineLogging:
    def __init__(self, pipeline_name: str, log_folder_path: str):
        """
        Initializes the logging for the ETL pipeline using loguru.

        Args:
            pipeline_name (str): Name of the pipeline.
            log_folder_path (str): Path to the folder where logs will be stored.
        """
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path

        # Ensure the log directory exists
        os.makedirs(log_folder_path, exist_ok=True)

        # Create log file name with timestamp for uniqueness
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.file_path = f"{self.log_folder_path}/{self.pipeline_name}_{timestamp}.log"

        # Configure Loguru with rotation and retention
        logger.add(
            self.file_path,
            rotation="500 MB",        # Rotate log after 500 MB
            retention="10 days",      # Retain log files for 10 days
            level="INFO",             # Log level
            format="{time} | {level} | {message}"
        )

        # Also log to console
        logger.add(lambda msg: print(msg, end=""))

        # Assign logger to the class instance
        self.logger = logger

    def get_logs(self) -> str:
        """Retrieve log contents from the generated log file."""
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())
