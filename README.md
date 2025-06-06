# 📦 File Upload and Query System

This project provides a Flask-based API and UI for uploading files from a local directory to MinIO and storing their metadata in a PostgreSQL database. It also supports querying file records via a web interface.

It includes:

- Automatic **upload of local files** to a MinIO bucket  
- **Metadata extraction** and upsert into PostgreSQL  
- A **Flask-based web UI** for querying file records

---

## 📁 Project Structure

```
project/
├── config.conf             # Configuration file for MinIO & PostgreSQL
├── functions.py            # File upload & DB insert/update logic
├── task.py                 # Flask app and main runner
├── test_unit.py            # Unit tests
└── templates/
    └── data_query.html     # Web UI for querying files
```

---

## ⚙️ Requirements

- Python 3.8+
- PostgreSQL
- MinIO (S3-compatible object storage)

Install dependencies:

```bash
pip install flask psycopg2 minio pytz
```

---

## ⚙️ Configuration

Edit the `config.conf` file with your settings:

```ini
[Minio]
folder_path = /path/to/your/files
bucket_name = main
host = localhost:9000
access_key = YOUR_ACCESS_KEY
secret_key = YOUR_SECRET_KEY

[Database]
db_name = files
db_host = localhost
db_port = 5432
db_user = your_user
db_password = your_password

[Table]
name = files

[Columns]
id = SERIAL PRIMARY KEY
bucket_name = VARCHAR(255)
storage_address = VARCHAR(255)
owner = VARCHAR(255)
file_name = VARCHAR(255)
file_size = VARCHAR(255)
upload_timestamp = timestamptz
hash_checksum = VARCHAR(255)
last_modified_timestamp = timestamptz
content_type = VARCHAR(255)
```

---

## 🚀 Usage

### 1. Upload Files & Start Server

Run the main script:

```bash
python task.py
```

This will:

- Upload all files from the local folder to MinIO
- Store or update metadata in PostgreSQL
- Launch the Flask web interface at `http://localhost:5000`

---

## 🌐 Web Interface

Access the app:

```
http://localhost:5000
```

**Features:**

- 🔍 Search by file name  
- 🏷️ Filter by content type (e.g., `.json`, `.csv`)  
- 📅 Filter by upload date range  
- 📊 View metadata in a clean, responsive table

---

## 🧪 Testing

Run unit tests with:

```bash
python test_unit.py
```

---

## ☁️ Setup: MinIO (via Docker)

Run MinIO locally:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=YOUR_ACCESS_KEY" \
  -e "MINIO_ROOT_PASSWORD=YOUR_SECRET_KEY" \
  quay.io/minio/minio server /data --console-address ":9001"
```

---

## 🗃️ Setup: PostgreSQL

Ensure PostgreSQL is running and create a database/table as configured.  
Use the credentials defined in `config.conf`.

---

## ✅ Example Workflow

```bash
# Start everything
python task.py

# Then go to browser:
http://localhost:5000

# Search for files like:
http://localhost:5000?name=report&type=csv
```

---

## 📌 Notes

- Uses **timezone-aware timestamps**  
- Metadata is **upserted** to avoid duplicates  
- Supports most file types with content-type auto-detection  
- All logic is centralized in `functions.py`

---

## 🤝 Contributing

Found a bug or have an idea? Open an issue or submit a PR!
