"""
Setup script para el proyecto ETL de entregas de productos.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Leer README para descripción larga
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name="etl-entregas-productos",
    version="1.0.0",
    author="Daniel Santos",
    author_email="daniel.santos.p@uni.pe",
    description="Sistema ETL para procesamiento de entregas de productos con PySpark y OmegaConf",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tu-usuario/etl-pyspark-project",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.4.0",
        "omegaconf>=2.3.0",
        "pandas>=2.0.3",
        "numpy>=1.24.3",
        "pydantic>=2.0.3",
        "click>=8.1.6",
        "loguru>=0.7.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.7.0",
            "flake8>=6.0.0",
            "mypy>=1.5.1",
            "jupyter>=1.0.0",
            "matplotlib>=3.7.2",
            "seaborn>=0.12.2",
        ],
        "quality": [
            "great-expectations>=0.17.15",
        ],
        "docs": [
            "mkdocs>=1.5.2",
            "mkdocs-material>=9.2.3",
        ],
    },
    entry_points={
        "console_scripts": [
            "etl-entregas=main:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json", "*.md"],
    },
    keywords="etl pyspark data-processing omegaconf spark big-data",
    project_urls={
        "Bug Reports": "https://github.com/tu-usuario/etl-pyspark-project/issues",
        "Source": "https://github.com/tu-usuario/etl-pyspark-project",
        "Documentation": "https://github.com/tu-usuario/etl-pyspark-project/docs",
    },
)