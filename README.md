# 🧪 AIOps Practical Examination — Continuous Evaluation Lab

> **Mode:** Local implementation in VS Code &nbsp;|&nbsp; **Evaluation:** Git commit history, file structure & implementation logic

---

## 📑 Table of Contents

1. [Instructions to Candidates](#-instructions-to-candidates)
2. [Task Overview](#-task-overview)
3. [Repository Structure](#-repository-structure)
4. [Task 1 — Log Parsing](#-task-1-log-parsing-and-rule-based-anomaly-detection)
5. [Task 2 — Isolation Forest](#-task-2-unsupervised-anomaly-detection-using-isolation-forest)
6. [Task 3 — Airflow DAG](#-task-3-telemetry-workflow-automation-using-apache-airflow)
7. [Task 4 — Kafka Streaming](#-task-4-real-time-event-streaming-with-apache-kafka)
8. [Submission Checklist](#-submission-checklist)
9. [Setup & Submission Guide](#-setup--submission-guide)
10. [Quick Commands (AIOps Lab Exam Project)](#-quick-commands--aiops-lab-exam-project)
11. [Pytest, Coverage & CI/CD Flow](#-pytest-coverage--cicd-flow)
12. [GitHub Actions — Result Check](#-github-actions--result-check)
13. [Branch + Pull Request Flow](#-branch--pull-request-flow)

---

## 📌 Instructions to Candidates

1. This practical exam evaluates your ability to implement **end-to-end AIOps tasks locally** using VS Code.
2. There are **NO automated online test cases** (e.g., HackerEarth style).
3. Evaluation is strictly based on your **Git commit history**, proper **file structure**, and **implementation logic**.
4. For every task:
   - Write your code in the **specified file**.
   - **Test it in the terminal**.
   - Make an **atomic git commit** with the **specified message**.
5. All tasks must be committed to the **`main`** branch before the submission deadline.

---

## 🗂️ Task Overview

| Task | Topic | Target File | Commit Message |
|------|-------|-------------|----------------|
| 1 | Log Parsing & Rule-Based Anomaly Detection | `task1_log_parsing/log_analysis.py` | `feat: task 1 completed log parsing and error counter` |
| 2 | Isolation Forest Anomaly Detection | `task2_ml_anomaly/anomaly_detection.py` | `feat: task 2 completed isolation forest outlier detection` |
| 3 | Airflow Monitoring DAG | `task3_airflow_pipeline/aiops_dag.py` | `feat: task 3 completed airflow monitoring dag pipeline` |
| 4 | Kafka Streaming & Monitoring | `task4_kafka_stream/kafka_pipeline.py` | `feat: task 4 completed kafka producer and consumer monitoring` |

---

## 📁 Repository Structure

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

## 🔍 Task 1: Log Parsing and Rule-Based Anomaly Detection

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

## 🌲 Task 2: Unsupervised Anomaly Detection using Isolation Forest

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

## ⚙️ Task 3: Telemetry Workflow Automation using Apache Airflow

**Target File:** `task3_airflow_pipeline/aiops_dag.py`

### Problem Description

Define an automated monitoring DAG in Apache Airflow named **`practical2_aiops_dag`**.

**DAG configuration:**

| Parameter | Value |
|-----------|-------|
| `dag_id` | `practical2_aiops_dag` |
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

## 📡 Task 4: Real-Time Event Streaming with Apache Kafka

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

## ✅ Submission Checklist

- [ ] Task 1 implemented, executed, and committed
- [ ] Task 2 implemented, executed, and committed
- [ ] Task 3 implemented, executed, and committed
- [ ] Task 4 implemented, executed, and committed
- [ ] One atomic commit per task, using the exact commit messages above
- [ ] All commits pushed to the `main` branch before the deadline

---

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
```

Then create and activate the virtual environment:

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Navigate and Create the Solution File

Go to the task folder and create your solution file:

```bash
cd task1_log_parsing
```

Create `log_analysis.py` using any editor.

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

✅ **Done!** Your solution is now live on your forked repository.

---

### 🛠️ Full Environment Setup (Step-by-Step, PowerShell)

```powershell
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
```

### 📦 Install All Packages

```powershell
pip install numpy pandas matplotlib seaborn scikit-learn scipy psutil regex requests python-dotenv apache-airflow kafka-python pydantic
```

### 🔄 Sync & Stage

```bash
git pull origin main --rebase
git push origin main

git add .
```

---

## ⚡ Quick Commands — AIOps Lab Exam Project

This repository contains the complete implementation of AIOps practical exam tasks.

> 📝 **Note:** Yeh section alternate folder names use karta hai (`task2_anomaly_detection/`, `dags/task3_dag.py`). Apne repo ke actual folder names ke hisaab se paths adjust karo.

Run these commands in your VS Code PowerShell terminal.

### 1. Clone & Enter Folder

```bash
git clone https://github.com/YOUR_USERNAME/REPO_NAME.git
cd REPO_NAME
git pull origin main
```

### 2. Virtual Environment Setup & Activation

```powershell
python -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
```

### 3. Install All Packages

```bash
pip install numpy pandas matplotlib scikit-learn apache-airflow kafka-python
```

### ▶️ Tasks Execution

**Task 1: Log Parsing**

```bash
python task1_log_parsing/log_analysis.py
```

**Task 2: Anomaly Detection**

```bash
python task2_anomaly_detection/anomaly_detection.py
```

**Task 3: Airflow DAG Setup**

```bash
python dags/task3_dag.py
```

### 📤 GitHub Submission Workflow (Using Exact File Paths)

**Submit Task 1:**

```bash
git add task1_log_parsing/log_analysis.py app.log
git commit -m "feat: task 1 completed log parsing and error counter"
git push origin main
```

**Submit Task 2:**

```bash
git add task2_anomaly_detection/anomaly_detection.py
git commit -m "feat: task 2 completed isolation forest anomaly detection"
git push origin main
```

**Submit Task 3:**

```bash
git add dags/task3_dag.py
git commit -m "feat: task 3 completed apache airflow dag pipeline"
git push origin main
```

### ⚠️ If Push Rejected (Fix)

```bash
git pull origin main --rebase
git push origin main
```

---

## 🧫 Pytest, Coverage & CI/CD Flow

### Step 1: Virtual Environment Setup

Terminal mein jaakar venv banao aur activate karo:

```bash
# 1. Virtual environment create karo
python -m venv .venv/calculations

# 2. Activate karo (Codespaces ya Linux/Mac par)
source .venv/calculations/bin/activate

# (Agar Windows PowerShell par ho to ye chalana: .venv\calculations\Scripts\Activate.ps1)
```

### Step 2: Dependencies Install Karo

Required packages aur coverage tools install karo:

```bash
# Repo ke requirements install karo
pip install -r requirements.txt

# Pytest aur coverage tools install karo
pip install pytest coverage pytest-cov
```

### Step 3: Tests Run Karo aur Coverage Check Karo

Unit tests run karke dekho ki 100% coverage aa rahi hai ya nahi:

```bash
# 1. Simple test run
pytest --verbose

# 2. Coverage check terminal par (100% check karne ke liye)
pytest --cov=src --verbose

# 3. Missing lines dekhne ke liye (agar 100% na ho)
pytest --cov=src --cov-report=term-missing
```

### Step 4: Workflows Add / Update Karo (CI/CD)

Jab tests pass ho jayein, to dono workflow files create ya check karke push karo:

```bash
# Status check karo
git status

# Changes stage karo
git add .

# Commit karo
git commit -m "add unit tests and workflows"

# Remote GitHub par push karo (Actions trigger karne ke liye)
git push origin main
```

### Step 5: Agar Bot Trigger Na Ho Toh (Empty Commit)

Agar GitHub Actions bot skip ho jaye ya trigger na kare, to ek empty commit push karke bot ko jaga do:

```bash
git commit --allow-empty -m "trigger step 0"
git push origin main
```

Bas itna hi exact flow hai jo humne abhi practical karte waqt terminal par use kiya tha!

---

## 🟢 GitHub Actions — Result Check

### Step 1: Repository ka "Actions" Tab Kholo

1. Apne browser mein apni GitHub repository ka page kholo.
2. Sabse upar top bar par menu hota hai:
   **Code | Issues | Pull requests | Actions | Projects | Settings**
3. Seedha **Actions** tab par click karo.

### Step 2: Running Workflow Status Dekho

Page refresh (`F5` ya `Ctrl + R`) karo. "All workflows" wali list mein sabse upar aapka latest commit dikhega:

| Status | Matlab |
|--------|--------|
| 🟡 **Yellow Spinning Circle** | GitHub ke server par tests abhi chal rahe hain. Kuchh mat dabao, bas 15–30 second wait karo aur page refresh karte raho. |
| 🟢 **Green Checkmark (✅)** | **Success!** Saare unit tests pass ho gaye aur CI/CD pipeline pass ho chuki hai. Exam ke hisaab se aapka kaam complete hai. |
| 🔴 **Red Cross (❌)** | Koi test fail ho gaya ya code mein koi syntax/coverage error aa gaya. |

### Step 3: Agar Red Cross (Fail) Aaye Toh Error Kaise Dekhein?

Agar red mark aata hai, toh ghabrao mat:

1. Us Red wale workflow run ke naam par click karo.
2. Left side mein job ka naam dikhega (jaise `build` ya `python-coverage`), uspe click karo.
3. Wahan steps ki list khul jayegi:
   - Jo step fail hua hoga (jaise `Test with pytest` ya `Fail if below threshold`), uske aage red cross hoga.
   - Us failed step par click karke dropdown kholo.
4. Terminal jaisa black console log khul jayega, jisme red color se saaf likha hoga ki kaunsa test **FAILED** hua ya coverage kitni percent aayi.
5. Wahi line number ya function apne VS Code mein theek karo, wapas commit & push karo, aur dubara Actions tab mein aakar green check verify kar lo.

---

## 🌿 Branch + Pull Request Flow

### Step 1: Terminal se Nayi Branch Push Karo (Best Command)

Ek hi line mein branch switch karke push karne ka sabse clean tareeqa:

```bash
# Nayi branch banao aur turant uspe shift ho jao
git checkout -b test-branch

# Code stage aur commit karo
git add .
git commit -m "add unit tests and coverage"

# Push karo
git push origin test-branch
```

### Step 2: GitHub Browser par Yellow Strip se 1-Click PR Banao

Yeh manual dropdown select karne se 10 guna fast hai:

1. Apne browser mein repo ka page open karo aur refresh (`F5`) karo.
2. Sabse upar screen par ek **Yellow alert bar** dikhegi:
   > `test-branch had recent pushes 1 minute ago`
3. Uske theek aage bane green button **Compare & pull request** par click kar do.
4. Naye page par bina koi title change kiye seedha neeche **Create pull request** green button daba do.

### Step 3: Result Check Karo (Usi Page Par)

PR create hote hi aapko Actions tab mein jaane ki bhi zaroorat nahi hai.

Usi Pull Request ke page par thoda neeche scroll karo:

- Wahan checks ka box apne aap aa jata hai.
- Pehle 🟡 (running) dikhega.
- 15–20 second mein **All checks have passed** ke sath 🟢 Green tick lag jayega.
- Coverage report ka comment bhi Mona bot usi PR ke neeche automatically post kar degi.

> 💡 Agar exam mein PR required ho, toh yeh **"Yellow Strip wala method"** sabse best, fast aur zero-error tareeqa hai!














1. PR ke liye Final Recap (Sirf 2 Min Ka Kaam)
Terminal aur browser me bas ye steps karne hain:

Terminal me commands chalao:

Bash
git checkout -b test-branch
git add .
git commit -m "completed tasks"
git push origin test-branch
GitHub browser par jao:

Repo refresh karo.

Upar aayi peeli (yellow) strip par Compare & pull request par click karo.

Neeche Create pull request daba do.

20 second me usi page par green check (All checks have passed) aa jayega. Bas done!


