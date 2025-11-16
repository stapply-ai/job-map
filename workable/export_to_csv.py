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

                # Workable structure: jobs can be a list directly, or under "jobs" key
                job_list = data if isinstance(data, list) else data.get("jobs", [])
                if not isinstance(job_list, list):
                    continue

                for job in job_list:
                    # Workable uses "url" (simple field name)
                    url = job.get("url", "")
                    title = job.get("title", "")

                    # Workable location is a dict with "city" key (not "name")
                    location = job.get("location", {})
                    if isinstance(location, dict):
                        location_str = location.get("city", "")
                    else:
                        location_str = str(location)

                    job_rows.append([url, title, location_str, company_name])

    print(f"Processed {len(job_rows)} total jobs")

    with open(jobs_csv_path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["url", "title", "location", "company"])
        writer.writerows(job_rows)


if __name__ == "__main__":
    main()
