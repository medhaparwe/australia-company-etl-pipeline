"""
Setup script for Australia Company ETL Pipeline.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read requirements
requirements_path = Path(__file__).parent / "requirements.txt"
if requirements_path.exists():
    requirements = [
        line.strip() 
        for line in requirements_path.read_text().splitlines() 
        if line.strip() and not line.startswith('#')
    ]
else:
    requirements = []

setup(
    name="australia-company-etl",
    version="1.0.0",
    description="ETL Pipeline for Australian Company Data from Common Crawl and ABR",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Data Engineering Team",
    author_email="data@example.com",
    url="https://github.com/yourusername/australia-company-etl",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "spark": [
            "pyspark>=3.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "etl-pipeline=pipeline:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    keywords="etl, data-engineering, entity-matching, common-crawl, australia",
)

