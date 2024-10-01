# Data Profiler API Service 

This is a FastAPI-based API for data profiling. It provides endpoints to perform data profiling, data cleansing, data manipulation, analytics and generate reports, and download them.

## Project Structure

The project is organized into the following structure:

- `app`: Contains the main FastAPI application and modules for database connections, data processing, report generation, and utility functions.
- `reports`: Directory to store generated data profiling reports.
- `venv`: Virtual environment for the project.
- `requirements.txt`: List of project dependencies.


### Prerequisites

- Python 3.7 or higher
- MongoDB

## Getting Started

1. Clone this repository:

```bash
git clone git@github.com:shahadulhaider/dataprofiler.git
```
2. Navigate to project Directory


```bash
cd dataprofiler
```

3. Set up Virtual Environment:

```bash
python -m venv venv
````

Activate the Virtual Environment:


- Linux/ Mac

```bash 
source venv/bin/activate
```

- Windows

```bash
.\venv\Scripts\activate
```

4. Install project dependencies using:


```bash
  pip install -r requirements.txt
```

5. Environment Variables:


```bash
cp .env.example .env
```

Update the variables. 

6. Running the Application:


```bash
uvicorn app.main:app --reload
```

The API will be accessible at `http://localhost:8000` 

## Endpoints

### Data Profile Endpoint
- Method: `POST`
- Endpoint: `/data-profile`
- Description: Perform data profiling based on provided data.

### Generate Report Endpoint
- Method: `POST`
- Endpoint: `/generate-report`
- Description: Generate a data profiling report based on provided data.

### Download Report Endpoint
- Method: `GET`
- Endpoint: `/download-report/{report_path:path}`
- Description: Download a previously generated data profiling report.
