# Project Setup Guide

This guide will help you set up the development environment for this project.

---

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.12.x** - [Download Python](https://www.python.org/downloads/)
- **Docker & Docker Compose** - [Download Docker](https://www.docker.com/products/docker-desktop)
- **MongoDB Compass** (optional, for database GUI) - [Download Compass](https://www.mongodb.com/products/compass)

---

## Getting Started

Follow these steps to set up your development environment:

### 1. Create a Virtual Environment

```bash
python -m venv venv
```

Activate the virtual environment:

**Windows:**
```bash
venv\Scripts\activate
```

**macOS/Linux:**
```bash
source venv/bin/activate
```

### 2. Install Python Dependencies

Navigate to the backend directory and install requirements:

```bash
cd backend
pip install -r requirements.txt
```

### 3. Start MongoDB

Navigate to the MongoDB setup directory and start the container:

```bash
cd setup/mongo
docker-compose up -d
```

Verify MongoDB is running:
```bash
docker ps
```

You should see a container named `mongo` running on port `27017`.

### 4. Start Weaviate

Navigate to the Weaviate setup directory and start the container:

```bash
cd setup/weaviate
docker-compose up -d
```

Verify Weaviate is running:
```bash
docker ps
```

You should see a container named `weaviate` running on ports `8080` and `50051`.

### 5. Connect to MongoDB

Open MongoDB Compass and connect using this connection string:

```
mongodb://admin:pulsebord@localhost:27017/datatodashboard?authSource=admin&authMechanism=SCRAM-SHA-256
```

---

## Troubleshooting

### MongoDB Authentication Failed

If you see an "Authentication Failed" error in MongoDB Compass, follow these steps:

#### Step 1: Connect via mongosh

Open a new terminal and connect using the MongoDB shell:

```bash
mongosh "mongodb://admin:pulsebord@localhost:27017/admin"
```

#### Step 2: Create Admin User (if needed)

If the `admin` user doesn't exist, create it:

```javascript
use admin
db.createUser({
  user: "admin",
  pwd: "pulsebord",
  roles: [ { role: "root", db: "admin" } ]
})
```

#### Step 3: Reconnect via Compass

Now try connecting again with MongoDB Compass using the connection string:

```
mongodb://admin:pulsebord@localhost:27017/datatodashboard?authSource=admin&authMechanism=SCRAM-SHA-256
```

---
