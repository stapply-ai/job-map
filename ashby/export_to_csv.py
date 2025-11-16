import os
import json
import csv

def main():
    companies_dir = os.path.join(os.path.dirname(__file__), "companies")
    jobs_csv_path = os.path.join(os.path.dirname(__file__), "jobs.csv")

    job_rows = []

    for filename in os.listdir(companies_dir):
        if filename.endswith(".json"):
            company_name = os.path.splitext(filename)[0]
            json_path = os.path.join(companies_dir, filename)
            with open(json_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception:
                    # skip files that are not valid JSON
                    continue

                # jobs can be a list directly, or under a key
                jobs = (
                    data.get("jobs") if isinstance(data, dict) and "jobs" in data else data
                )
                if not isinstance(jobs, list):
                    continue

                for job in jobs:
                    # Ashby JSON uses camelCase: jobUrl, not url
                    url = job.get("jobUrl", job.get("applyUrl", ""))
                    title = job.get("title", "")
                    location = job.get("location", "")
                    job_rows.append([url, title, location, company_name])

    print(f"Processed {len(job_rows)} total jobs")

    with open(jobs_csv_path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["url", "title", "location", "company"])
        writer.writerows(job_rows)

if __name__ == "__main__":
    main()
