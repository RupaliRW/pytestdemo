# PySpark Orders ETL with PyTest

## Overview

This project demonstrates:

* PySpark ETL with explicit schema
* Data quality transformations
* Unit testing using PyTest
* Clean project structure using `src/` and `tests/`

---

## Project Structure

```
pyestdemo/
│
├── src/
│   └── transformation.py
│
├── tests/
│   ├── conftest.py
│   ├── test_transformation.py
│   └── data/
│       └── orders.csv
│
├── requirements.txt
└── README.md
```

---

## Prerequisites

* Python **3.8 – 3.11**
* Java **8 or 11** (required for Spark)
* pip

---

## Setup Instructions

### 1. Clone repository (or download project)

```
git clone <repo-url>
cd pyestdemo
```

---

### 2. Create virtual environment

```
python -m venv venv
```

---

### 3. Activate environment

**Windows**

```
venv\Scripts\activate
```

**Mac/Linux**

```
source venv/bin/activate
```

---

### 4. Install dependencies

```
pip install -r requirements.txt
```

---

##  Run PySpark ETL Job

```
python src/transformation.py
```

Output will be created in:

```
output/orders/
```

---

## Run Tests

```
pytest -v
```

Expected:

```
collected X items
... PASSED
```

---

## Test Coverage

* Schema validation
* Column data types
* Null handling
* Negative value filtering
* Business logic validation (COMPLETED orders only)

---

## Important Notes

* `conftest.py` is used to manage import paths (no PYTHONPATH needed)
* Always run pytest from **project root**
* Spark runs in **local mode** for testing

---

---

## Key Concepts Demonstrated

* Explicit schema enforcement (avoids schema drift)
* Testable PySpark transformations
* Separation of concerns (ETL vs tests)
* Production-style project structure

---
