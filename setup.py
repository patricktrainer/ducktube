from setuptools import setup, find_packages

setup(
    name="ducktube",
    version="0.1.0",
    description="Watch videos with DuckDB WASM",
    author="Patrick Trainer",
    packages=find_packages(),
    package_data={
        "ducktube": ["spec.json"],
    },
    install_requires=[
        "airbyte-cdk",
        "duckdb",
        "airbyte",
        "yt-dlp",
        "opencv-python",
        "numpy"
    ],
    entry_points={
        'console_scripts': [
            'ducktube=ducktube.__main__:main',
        ],
    },
)