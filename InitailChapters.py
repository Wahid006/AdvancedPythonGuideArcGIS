import os
from dataclasses import dataclass
import arcpy

class DirectoryScanner:
    def __init__(self, path: str, recursive: bool = True):
        self.path = path
        self.recursive = recursive 

    def scan(self):
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"Directory {self.directory} does not exist.")
        return [os.path.join(self.directory, f) for f in os.listdir(self.directory) if os.path.isfile(os.path.join(self.directory, f))]