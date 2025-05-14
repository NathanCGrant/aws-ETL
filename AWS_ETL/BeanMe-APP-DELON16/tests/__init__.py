import pytest
import os

if __name__ == "__main__":
    folder = os.path.dirname(__file__)
    # Run pytest in the current folder with verbose output
    pytest.main([folder, '-v'])