import logging
import time

class PipelineLogging:
    def __init__(self, pipeline_name: str, log_folder_path: str):
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path
        logger = logging.getLogger(pipeline_name)
        logger.setLevel(logging.INFO)
        self.file_path = f"{self.log_folder_path}/{self.pipeline_name}_{time.time()}.log"

        # File handler
        file_handler = logging.FileHandler(self.file_path)
        file_handler.setLevel(logging.INFO)

        # Stream handler for console output
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        self.logger = logger

    def get_logs(self) -> str:
        """Retrieve log contents."""
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())
