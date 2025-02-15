
# Data Processing and Analysis with Apache Spark

## Overview

This project demonstrates the use of Apache Spark for processing and analyzing game match data. The script performs various data operations, including broadcast joins, bucketed table creation, data aggregation, and performance comparison of different data processing strategies.

## Features

- **Broadcast Joins**: Efficiently joins large datasets with smaller lookup tables using broadcast joins.
- **Bucketed Tables**: Creates and manages bucketed tables to enhance query performance.
- **Data Aggregation**: Computes aggregated statistics such as average kills per game, most common playlist, and most played map.
- **DataFrame Operations**: Reads, writes, and transforms data using Spark DataFrames.
- **Performance Comparison**: Compares file sizes of different bucketed DataFrames to analyze and optimize data storage.

## Prerequisites

- Apache Spark
- Python 3.x
- PySpark
