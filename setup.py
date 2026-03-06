from setuptools import setup, find_packages

setup(
    name="urban-energy-etl",
    version="0.1.0",
    description="An ETL pipeline for processing urban energy consumption data",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "python-dotenv>=1.0.0",
        "pandas>=2.0.3",
        "numpy>=1.24.3",
        "requests>=2.31.0",
        "pydantic>=2.3.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
        ],
    },
    python_requires=">=3.8",
)
