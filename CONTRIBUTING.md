# 🤝 Contributing to DBDuck

Thank you for your interest in contributing to DBDuck 🚀
DBDuck aims to provide a **unified API for multiple databases** (SQL, MongoDB, Neo4j, Qdrant).

---

## 📌 Ways to Contribute

You can contribute in multiple ways:

* 🐞 Bug Fixes
* ✨ New Features
* 📄 Documentation Improvements
* ⚡ Performance Optimization
* 🧪 Writing Tests

---

## ⚙️ Project Setup

### 1. Fork the Repository

Click **Fork** on GitHub.

---

### 2. Clone Your Fork

```bash
git clone https://github.com/YOUR_USERNAME/DBDuck.git
cd DBDuck
```

---

### 3. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows
```

---

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🌱 Branching Strategy

* `main` → stable code
* `dev` → development
* `feature/*` → new features
* `fix/*` → bug fixes

Example:

```bash
git checkout -b feature/mongodb-pagination
```

---

## 🧑‍💻 Coding Guidelines

* Follow **PEP8 (Python standard)**
* Use meaningful variable names
* Write modular, reusable code
* Add docstrings

Example:

```python
def connect_db(config: dict) -> Connection:
    """Connect to database using config"""
    pass
```

---

## 🧪 Testing

Before submitting PR:

```bash
pytest
```

✔ Ensure:

* No errors
* No broken features

---

## 📝 Commit Message Format

Use clear commit messages:

* `feat: add Neo4j async support`
* `fix: resolve MongoDB connection bug`
* `docs: update README examples`

---

## 🔄 Pull Request Process

1. Push your branch
2. Create Pull Request
3. Explain clearly:

   * What you did
   * Why it’s needed

Example:

> Added async support for Qdrant adapter to improve performance

---

## 📌 Issue Guidelines

Before creating issue:

* Check existing issues
* Use clear title

Example:

* ❌ “Error”
* ✅ “MongoDB adapter fails on large dataset”

---

## 🏗️ Architecture Overview

DBDuck follows:

* Adapter Pattern for DBs
* Unified Query Interface
* Extensible backend system

---

## 💡 Contribution Ideas

Good first contributions:

* Add new database adapter
* Improve error handling
* Add logging
* Write test cases
* Improve docs

---

## 📢 Code of Conduct

* Be respectful
* No toxic behavior
* Help others

---

## ❤️ Thank You

Your contributions make DBDuck better 🚀
# 🤝 Contributing to DBDuck

Thank you for your interest in contributing to DBDuck 🚀
DBDuck aims to provide a **unified API for multiple databases** (SQL, MongoDB, Neo4j, Qdrant).

---

## 📌 Ways to Contribute

You can contribute in multiple ways:

* 🐞 Bug Fixes
* ✨ New Features
* 📄 Documentation Improvements
* ⚡ Performance Optimization
* 🧪 Writing Tests

---

## ⚙️ Project Setup

### 1. Fork the Repository

Click **Fork** on GitHub.

---

### 2. Clone Your Fork

```bash
git clone https://github.com/Veeresh-Hanni/DBDuck.git
cd DBDuck
```

---

### 3. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows
```

---

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🌱 Branching Strategy

* `main` → stable code
* `dev` → development
* `feature/*` → new features
* `fix/*` → bug fixes

Example:

```bash
git checkout -b feature/mongodb-pagination
```

---

## 🧑‍💻 Coding Guidelines

* Follow **PEP8 (Python standard)**
* Use meaningful variable names
* Write modular, reusable code
* Add docstrings

Example:

```python
def connect_db(config: dict) -> Connection:
    """Connect to database using config"""
    pass
```

---

## 🧪 Testing

Before submitting PR:

```bash
pytest
```

✔ Ensure:

* No errors
* No broken features

---

## 📝 Commit Message Format

Use clear commit messages:

* `feat: add Neo4j async support`
* `fix: resolve MongoDB connection bug`
* `docs: update README examples`

---

## 🔄 Pull Request Process

1. Push your branch
2. Create Pull Request
3. Explain clearly:

   * What you did
   * Why it’s needed

Example:

> Added async support for Qdrant adapter to improve performance

---

## 📌 Issue Guidelines

Before creating issue:

* Check existing issues
* Use clear title

Example:

* ❌ “Error”
* ✅ “MongoDB adapter fails on large dataset”

---

## 🏗️ Architecture Overview

DBDuck follows:

* Adapter Pattern for DBs
* Unified Query Interface
* Extensible backend system

---

## 💡 Contribution Ideas

Good first contributions:

* Add new database adapter
* Improve error handling
* Add logging
* Write test cases
* Improve docs

---

## 📢 Code of Conduct

* Be respectful
* No toxic behavior
* Help others

---

## ❤️ Thank You

Your contributions make DBDuck better 🚀

