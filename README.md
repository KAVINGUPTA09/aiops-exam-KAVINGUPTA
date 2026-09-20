# AIOps Practical Examination — Continuous Evaluation Lab

> **Mode:** Local implementation in VS Code | **Evaluation:** Git commit history, file structure & implementation logic

---

## Instructions to Candidates

1. This practical exam evaluates your ability to implement **end-to-end AIOps tasks locally** using VS Code.
2. There are **NO automated online test cases** (e.g., HackerEarth style).
3. Evaluation is strictly based on your **Git commit history**, proper **file structure**, and **implementation logic**.
4. For every task:
   - Write your code in the **specified file**.
   - **Test it in the terminal**.
   - Make an **atomic git commit** with the **specified message**.
5. All tasks must be committed to the **`main`** branch before the submission deadline.

---

## Task Overview

| Task | Topic | Target File | Commit Message |
|------|-------|-------------|----------------|
| 1 | Log Parsing & Rule-Based Anomaly Detection | `task1_log_parsing/log_analysis.py` | `feat: task 1 completed log parsing and error counter` |
| 2 | Isolation Forest Anomaly Detection | `task2_ml_anomaly/anomaly_detection.py` | `feat: task 2 completed isolation forest outlier detection` |
| 3 | Airflow Monitoring DAG | `task3_airflow_pipeline/aiops_dag.py` | `feat: task 3 completed airflow monitoring dag pipeline` |
| 4 | Kafka Streaming & Monitoring | `task4_kafka_stream/kafka_pipeline.py` | `feat: task 4 completed kafka producer and consumer monitoring` |

---

## Repository Structure

```text
.
├── application.log                     # Input data for Task 1
├── README.md
├── task1_log_parsing/
│   └── log_analysis.py
├── task2_ml_anomaly/
│   └── anomaly_detection.py
├── task3_airflow_pipeline/
│   └── aiops_dag.py
└── task4_kafka_stream/
    └── kafka_pipeline.py
```

---

## Task 1: Log Parsing and Rule-Based Anomaly Detection

**Target File:** `task1_log_parsing/log_analysis.py`
**Input Data File:** `application.log` (present in repository root)

### Problem Description

You are provided with an enterprise application log file, `application.log`.
Write a Python script that:

1. Reads all log entries **line by line**.
2. Counts the **total number of lines** that have the `ERROR` log level.
3. Extracts the hour-minute timestamp (`HH:MM`) from each error entry and counts the **number of errors per minute** using `collections.Counter`.
4. Implements a **rule-based threshold alert** (`threshold = 3`): if any minute has **strictly more than 3 errors**, display an alert indicating an anomaly with the **exact minute and count**.

### Expected Deliverable & Commit

- Create and execute: `task1_log_parsing/log_analysis.py`
- **Commit Message:**
  ```
  feat: task 1 completed log parsing and error counter
  ```

---

## Task 2: Unsupervised Anomaly Detection using Isolation Forest

**Target File:** `task2_ml_anomaly/anomaly_detection.py`

### Problem Description

A production application server has recorded telemetry response time metrics (in milliseconds) across **20 continuous sampling intervals**:

```python
response_time = [
    120, 125, 118, 130, 122, 127, 124, 121, 129, 126,
    123, 128, 125, 122, 131, 700, 127, 119, 650, 124
]
```
```python
cpu_usage = [
    32, 35, 34, 36, 33, 35, 37, 34, 36, 35,
    34, 33, 36, 35, 34, 92, 35, 33, 95, 34
]
```
While standard latencies fall between **118 ms and 131 ms**, specific intervals show severe latency degradation.

Write a Python script that:

1. Formats the data into a **2-dimensional NumPy array** compatible with scikit-learn.
2. Fits an `IsolationForest` estimator from `sklearn.ensemble` using `contamination=0.1` and `random_state=42`.
3. Predicts the operational labels for each point (`1` for normal, `-1` for anomaly).
4. Prints each value alongside its classified operational status (`NORMAL` or `ANOMALY DETECTED`).

### Expected Deliverable & Commit

- Create and execute: `task2_ml_anomaly/anomaly_detection.py`
- **Commit Message:**
  ```
  feat: task 2 completed isolation forest outlier detection
  ```

---

## Task 3: Telemetry Workflow Automation using Apache Airflow

**Target File:** `task3_airflow_pipeline/aiops_dag.py`

### Problem Description

Define an automated monitoring DAG in Apache Airflow named **`practical2_aiops_dag`**.

**DAG configuration:**

| Parameter | Value |
|-----------|-------|
|dag id|
| `start_date` | September 14, 2026 |
| `schedule` | `None` |
| `catchup` | `False` |

Using `PythonOperator`, implement **four distinct tasks**:

| Task ID | Responsibility |
|---------|----------------|
| `collect_data` | Prints status of telemetry collection (CPU, memory, error rate). |
| `process_data` | Prints status indicating data transformation and parsing. |
| `detect_anomalies` | Compares a metric (e.g., `CPU = 85`) against threshold `80`, printing an alert if exceeded. |
| `saving_results` | Prints a confirmation message that outputs are saved to the database. |

Set **linear dependency orchestration** using bitshift operators:

```python
collect >> process >> detect >> saving
```

### Expected Deliverable & Commit

- Create and execute: `task3_airflow_pipeline/aiops_dag.py`
- **Commit Message:**
  ```
  feat: task 3 completed airflow monitoring dag pipeline
  ```

---

## Task 4: Real-Time Event Streaming with Apache Kafka

**Target File:** `task4_kafka_stream/kafka_pipeline.py`

### Problem Description

Implement real-time metric streaming and threshold monitoring using Kafka:

1. Configure a **`KafkaProducer`** serializing JSON payloads to topic **`server-telemetry`**.
2. Produce telemetry events containing `server_id`, `cpu_usage`, and `timestamp`.
3. Configure a **`KafkaConsumer`** deserializing incoming JSON records from `server-telemetry`.
4. Check consumed events:
   - If `cpu_usage > 80` → output a **`CRITICAL ALERT`**
   - Otherwise → output status **`OK`**

### Expected Deliverable & Commit

- Create and execute: `task4_kafka_stream/kafka_pipeline.py`
- **Commit Message:**
  ```
  feat: task 4 completed kafka producer and consumer monitoring
  ```

---

## Submission Checklist

- [ ] Task 1 implemented, executed, and committed
- [ ] Task 2 implemented, executed, and committed
- [ ] Task 3 implemented, executed, and committed
- [ ] Task 4 implemented, executed, and committed
- [ ] One atomic commit per task, using the exact commit messages above
- [ ] All commits pushed to the `main` branch before the deadline





## 🚀 Setup & Submission Guide

Follow these steps to complete and submit the assignment.

### 1. Fork the Repository
Open the assignment repository in your browser and click the **Fork** button at the top-right corner.

### 2. Clone to Your Local Machine
Copy your forked repo's URL, then clone it and move into the project folder:

```bash
git clone https://github.com/YOUR_USERNAME/REPO_NAME.git
cd REPO_NAME
code .


python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Navigate and Create the Solution File
Go to the task folder and create your solution file:

```bash
cd task1_log_parsing
```

Create `log_analysis.py` using any editor:


### 4. Run and Verify the Script
Make sure the script runs and produces output without errors:

```bash
python log_analysis.py
```

### 5. Commit and Push Your Changes
Stage the file, commit with the required message, and push to your remote repo:

```bash
git status
git add task1_log_parsing/log_analysis.py
git commit -m "feat: task 1 completed log parsing and error counter"
git push origin main
```

---

✅ **Done!** Your solution is now live on your forked repository.

# 1. Cloned repo ke folder mein jao
cd REPO_NAME

# 2. Virtual environment banao
python -m venv venv

# 3. Execution policy bypass karo (agar script error aaye)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 4. Environment activate karo
.\venv\Scripts\Activate.ps1

# 5. Pip upgrade karo
python -m pip install --upgrade pip

# 6. Agar requirements.txt file repo mein hai:
pip install -r requirements.txt

# (OR) Agar requirements.txt file nahi hai, toh direct packages daalo:
pip install numpy matplotlib scikit-learn

#all files
pip install numpy pandas matplotlib seaborn scikit-learn scipy psutil regex requests python-dotenv apache-airflow kafka-python pydantic


git pull origin main --rebase
git push origin main

git add .

















#more github commands

# AIOps Lab Exam Project

This repository contains the complete implementation of AIOps practical exam tasks.

---

## Environment Setup & Commands

Run these commands in your VS Code PowerShell terminal:

### 1. Clone & Enter Folder
git clone https://github.com/YOUR_USERNAME/REPO_NAME.git
cd REPO_NAME
git pull origin main

### 2. Virtual Environment Setup & Activation
python -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1

### 3. Install All Packages
pip install numpy pandas matplotlib scikit-learn apache-airflow kafka-python

---

## Tasks Execution

### Task 1: Log Parsing
python task1_log_parsing/log_analysis.py

### Task 2: Anomaly Detection
python task2_anomaly_detection/anomaly_detection.py

### Task 3: Airflow DAG Setup
python dags/task3_dag.py

---

## GitHub Submission Workflow (Using Exact File Paths)

### Submit Task 1:
git add task1_log_parsing/log_analysis.py app.log
git commit -m "feat: task 1 completed log parsing and error counter"
git push origin main

### Submit Task 2:
git add task2_anomaly_detection/anomaly_detection.py
git commit -m "feat: task 2 completed isolation forest anomaly detection"
git push origin main

### Submit Task 3:
git add dags/task3_dag.py
git commit -m "feat: task 3 completed apache airflow dag pipeline"
git push origin main

---

## If Push Rejected (Fix):
git pull origin main --rebase
git push origin main
