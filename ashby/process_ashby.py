import json
import logging
import time
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4
import sys

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import (
    create_engine,
    text,
    Column,
    String,
    Boolean,
    DateTime,
    Float,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import declarative_base, sessionmaker
from openai import OpenAI

from models.ashby import AshbyApiResponse, AshbyJob
from models.db import DatabaseJob

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

Base = declarative_base()


class CompanyTable(Base):
    __tablename__ = "companies"
    id = Column(PGUUID(as_uuid=True), primary_key=True)
    name = Column(String, unique=True, nullable=False)


class JobTable(Base):
    __tablename__ = "jobs"
    id = Column(PGUUID(as_uuid=True), primary_key=True)
    url = Column(String, nullable=False)
    title = Column(String, nullable=False)
    location = Column(String)
    company = Column(String, nullable=False)
    description = Column(Text)
    employment_type = Column(String)
    industry = Column(String)
    embedding = Column(Text)
    posted_at = Column(DateTime)
    created_at = Column(DateTime, server_default=text("now()"))
    source = Column(String)
    is_active = Column(Boolean, default=True)
    added_by_user = Column(Boolean, default=False)
    remote = Column(Boolean)
    wfh = Column(Boolean)
    application_url = Column(String)
    language = Column(String)
    title_embedding = Column(Text)
    verified_at = Column(DateTime)
    lon = Column(Float)
    lat = Column(Float)
    country = Column(String)
    point = Column(String)
    salary_min = Column(Float)
    salary_max = Column(Float)
    salary_currency = Column(String)
    salary_period = Column(String)
    city = Column(String)
    ats_type = Column(String)
    company_id = Column(PGUUID(as_uuid=True))


def get_or_create_company(session, company_name: str) -> UUID:
    """Get existing company or create new one."""
    # Trim company name
    company_name = company_name.strip()
    logger.debug(f"Getting or creating company: {company_name}")
    company = session.query(CompanyTable).filter_by(name=company_name).first()
    if company:
        logger.info(f"Found existing company: {company_name} (ID: {company.id})")
        return company.id

    logger.info(f"Creating new company: {company_name}")
    new_company = CompanyTable(id=uuid4(), name=company_name)
    session.add(new_company)
    session.commit()
    logger.info(f"Created company: {company_name} (ID: {new_company.id})")
    return new_company.id


def generate_embedding(
    client: OpenAI, text: str, embedding_type: str = "general"
) -> Optional[str]:
    """
    Generate embedding using OpenAI API and wait for response.

    Args:
        client: OpenAI client instance
        text: Text to generate embedding for
        embedding_type: Type of embedding (for logging purposes)

    Returns:
        String representation of embedding vector or None if failed
    """
    if not text or not text.strip():
        logger.warning(f"Empty text provided for {embedding_type} embedding")
        return None

    try:
        logger.debug(
            f"Generating {embedding_type} embedding for text length: {len(text)}"
        )

        # Make synchronous API call and wait for response
        response = client.embeddings.create(input=text, model="text-embedding-3-small")

        # Extract embedding vector from response
        embedding_values = response.data[0].embedding

        logger.debug(
            f"Successfully generated {embedding_type} embedding with {len(embedding_values)} dimensions"
        )

        # Convert to string representation for PostgreSQL
        return str(embedding_values)

    except Exception as e:
        logger.error(f"Error generating {embedding_type} embedding: {e}", exc_info=True)
        return None


def convert_ashby_to_database_job(
    ashby_job: AshbyJob,
    company_name: str,
    company_id: UUID,
    description_embedding: Optional[str],
    title_embedding: Optional[str],
) -> DatabaseJob:
    """Convert AshbyJob to DatabaseJob."""
    logger.debug(f"Converting AshbyJob to DatabaseJob: {ashby_job.title}")
    return DatabaseJob(
        id=UUID(ashby_job.id),
        url=ashby_job.job_url,
        title=ashby_job.title,
        location=ashby_job.location,
        company=company_name,
        description=ashby_job.description_plain,
        employment_type=ashby_job.employment_type,
        remote=ashby_job.is_remote,
        application_url=ashby_job.apply_url,
        posted_at=ashby_job.published_at,
        source="ashby",
        ats_type="ashby",
        company_id=company_id,
        embedding=description_embedding,
        title_embedding=title_embedding,
        is_active=ashby_job.is_listed,
    )


def load_processed_companies(checkpoint_file: Path) -> set:
    """Load list of already processed companies from checkpoint file."""
    if checkpoint_file.exists():
        with open(checkpoint_file, "r") as f:
            processed = set(line.strip() for line in f if line.strip())
        logger.info(
            f"Loaded {len(processed)} previously processed companies from checkpoint"
        )
        return processed
    logger.info("No checkpoint file found, starting fresh")
    return set()


def mark_company_processed(checkpoint_file: Path, company_name: str):
    """Append company name to checkpoint file."""
    with open(checkpoint_file, "a") as f:
        f.write(f"{company_name}\n")
    logger.debug(f"Marked {company_name} as processed in checkpoint file")


def check_job_exists_by_url(
    session, url: str, ats_type: str, company_name: str
) -> Optional[JobTable]:
    """Check if job exists in DB by URL, ats_type, and company_name."""
    existing = (
        session.query(JobTable)
        .filter_by(url=url, ats_type=ats_type, company=company_name)
        .first()
    )
    return existing


def deactivate_removed_jobs(
    session,
    company_id: UUID,
    ats_type: str,
    current_job_urls: set,
    company_name: str,
):
    """Mark jobs as inactive if they exist in DB but not in current scrape."""
    # Query all jobs for this company and ATS type
    db_jobs = (
        session.query(JobTable)
        .filter_by(company_id=company_id, ats_type=ats_type)
        .all()
    )

    deactivated_count = 0
    for db_job in db_jobs:
        if db_job.url not in current_job_urls:
            if db_job.is_active:
                db_job.is_active = False
                deactivated_count += 1
                logger.debug(f"  Deactivating job: {db_job.title} (URL: {db_job.url})")

    if deactivated_count > 0:
        session.commit()
        logger.info(
            f"  Deactivated {deactivated_count} jobs that are no longer listed for {company_name}"
        )
    else:
        logger.debug(f"  No jobs to deactivate for {company_name}")


def process_ashby_companies(
    database_url: str, openai_api_key: str, companies_folder: str = None
):
    """
    Process Ashby company JSON files and save to database.

    Args:
        database_url: PostgreSQL connection string
        openai_api_key: OpenAI API key
        companies_folder: Path to the folder containing company JSON files
    """
    # Default to companies folder relative to this script
    if companies_folder is None:
        companies_folder = str(Path(__file__).parent / "companies")

    # Fix postgres:// to postgresql:// for SQLAlchemy compatibility
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    logger.info("Starting Ashby companies processing")
    logger.info(f"Database URL: {database_url[:20]}...")
    logger.info(f"Companies folder: {companies_folder}")

    # Setup checkpoint file
    checkpoint_file = Path(__file__).parent / "processed_companies.txt"
    processed_companies = load_processed_companies(checkpoint_file)
    logger.info(f"Checkpoint file: {checkpoint_file}")

    # Initialize database connection
    logger.info("Initializing database connection...")
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Database connection established")

    # Initialize OpenAI client
    logger.info("Initializing OpenAI client...")
    openai_client = OpenAI(api_key=openai_api_key)
    logger.info("OpenAI client initialized")

    companies_path = Path(companies_folder)

    if not companies_path.exists():
        logger.error(f"Folder '{companies_folder}' does not exist")
        return

    logger.info(f"Scanning for JSON files in: {companies_path}")
    json_files = list(companies_path.glob("*.json"))
    logger.info(f"Found {len(json_files)} JSON files to process")

    total_jobs_processed = 0
    total_companies_processed = 0
    total_companies_skipped = 0
    total_errors = 0

    # Iterate through all JSON files
    for json_file in json_files:
        try:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing file: {json_file.name}")

            # Extract company name from filename and capitalize first letter
            company_name = json_file.stem.replace("-", " ").replace("_", " ")
            company_name = (
                company_name[0].upper() + company_name[1:]
                if company_name
                else company_name
            )

            logger.info(f"Company name: {company_name}")

            # Skip if already processed
            if company_name in processed_companies:
                logger.info(f"⏭  Skipping {company_name} (already processed)")
                total_companies_skipped += 1
                continue

            # Get or create company
            company_id = get_or_create_company(session, company_name)

            # Load JSON file
            logger.debug(f"Loading JSON file: {json_file}")
            with open(json_file, "r", encoding="utf-8") as f:
                company_data = json.load(f)
            logger.debug(f"Loaded JSON file successfully")

            # Parse using Pydantic model
            logger.debug("Parsing JSON with Pydantic model...")
            ashby_response = AshbyApiResponse(**company_data)
            logger.debug("Parsed successfully")

            jobs_count = len(ashby_response.jobs)
            logger.info(f"Found {jobs_count} jobs for {company_name}")

            # Track URLs for diff logic
            current_job_urls = set()

            # Process each job
            for idx, ashby_job in enumerate(ashby_response.jobs, 1):
                try:
                    logger.info(f"  [{idx}/{jobs_count}] Processing: {ashby_job.title}")

                    # Check if job URL already exists in DB
                    existing_job_by_url = check_job_exists_by_url(
                        session, ashby_job.job_url, "ashby", company_name
                    )

                    # Generate embeddings only if job doesn't exist or needs update
                    description_embedding = None
                    title_embedding = None

                    if existing_job_by_url:
                        # Job exists - keep existing embeddings, skip generation
                        logger.info(
                            f"    Job URL already exists in DB, skipping embedding generation"
                        )
                        description_embedding = existing_job_by_url.embedding
                        title_embedding = existing_job_by_url.title_embedding
                    else:
                        # Generate description embedding (first API call - wait for response)
                        if (
                            ashby_job.description_plain
                            and ashby_job.description_plain.strip()
                        ):
                            logger.info(f"    Generating description embedding...")
                            description_embedding = generate_embedding(
                                openai_client,
                                ashby_job.description_plain,
                                embedding_type="description",
                            )
                            if description_embedding:
                                logger.info(f"    ✓ Description embedding complete")
                            else:
                                logger.warning(
                                    f"    ⚠ Failed to generate description embedding"
                                )
                        else:
                            logger.warning(
                                f"    ⚠ No description available for embedding"
                            )

                        # Generate title+location embedding (second API call - wait for response)
                        title_location_text = f"{ashby_job.title}; {ashby_job.location}"
                        logger.info(f"    Generating title embedding...")
                        title_embedding = generate_embedding(
                            openai_client, title_location_text, embedding_type="title"
                        )
                        if title_embedding:
                            logger.info(f"    ✓ Title embedding complete")
                        else:
                            logger.warning(f"    ⚠ Failed to generate title embedding")

                    # Add URL to tracking set for diff logic
                    current_job_urls.add(ashby_job.job_url)

                    # Convert to DatabaseJob (with embeddings)
                    logger.debug("Converting to DatabaseJob...")
                    db_job = convert_ashby_to_database_job(
                        ashby_job,
                        company_name,
                        company_id,
                        description_embedding,
                        title_embedding,
                    )

                    # Verify embeddings are present before saving
                    embeddings_status = []
                    if db_job.embedding:
                        embeddings_status.append("description✓")
                    else:
                        embeddings_status.append("description✗")

                    if db_job.title_embedding:
                        embeddings_status.append("title✓")
                    else:
                        embeddings_status.append("title✗")

                    logger.debug(f"    Embeddings: {' '.join(embeddings_status)}")

                    # Insert into database
                    job_dict = db_job.model_dump(exclude_none=False)
                    logger.debug(f"Job dict created with {len(job_dict)} fields")

                    # Check if job already exists by ID
                    logger.debug(f"Checking if job exists: {job_dict['id']}")
                    existing = (
                        session.query(JobTable).filter_by(id=job_dict["id"]).first()
                    )
                    if existing:
                        # Update existing job (including setting is_active=True if it was inactive)
                        logger.debug("Job exists, updating...")
                        for key, value in job_dict.items():
                            setattr(existing, key, value)
                        # Ensure job is marked as active since it's in current scrape
                        existing.is_active = db_job.is_active
                        logger.info(f"    ✓ Updated existing job: {ashby_job.title}")
                    else:
                        # Insert new job
                        logger.debug("Job is new, inserting...")
                        new_job = JobTable(**job_dict)
                        session.add(new_job)
                        logger.info(f"    ✓ Inserted new job: {ashby_job.title}")

                    session.commit()
                    logger.debug("Changes committed to database")
                    total_jobs_processed += 1

                    # Wait 3 seconds before processing next job to avoid rate limits (only if generating embeddings)
                    if not existing_job_by_url and idx < jobs_count:
                        logger.debug("Waiting 3 seconds before next job...")
                        time.sleep(3)

                except Exception as e:
                    logger.error(
                        f"    ✗ Error processing job '{ashby_job.title}': {e}",
                        exc_info=True,
                    )
                    session.rollback()
                    total_errors += 1
                    continue

            # After processing all jobs, check for removed listings and deactivate them
            try:
                logger.info(f"Checking for removed listings for {company_name}...")
                deactivate_removed_jobs(
                    session, company_id, "ashby", current_job_urls, company_name
                )
            except Exception as e:
                logger.error(
                    f"Error deactivating removed jobs for {company_name}: {e}",
                    exc_info=True,
                )
                session.rollback()

            total_companies_processed += 1

            # Mark company as successfully processed
            mark_company_processed(checkpoint_file, company_name)
            logger.info(f"✓ Completed processing {company_name}")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {json_file.name}: {e}", exc_info=True)
            session.rollback()
            total_errors += 1
        except Exception as e:
            logger.error(f"Error processing {json_file.name}: {e}", exc_info=True)
            session.rollback()
            total_errors += 1

    session.close()
    logger.info(f"\n{'=' * 60}")
    logger.info("Processing Summary:")
    logger.info(f"  Total companies processed: {total_companies_processed}")
    logger.info(f"  Total companies skipped: {total_companies_skipped}")
    logger.info(f"  Total jobs processed: {total_jobs_processed}")
    logger.info(f"  Total errors: {total_errors}")
    logger.info(f"  Checkpoint file: {checkpoint_file}")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse
    import os
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Process Ashby companies and save to database"
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection string (default: from DATABASE_URL env var)",
    )
    parser.add_argument(
        "--openai-api-key",
        default=os.getenv("OPENAI_API_KEY"),
        help="OpenAI API key (default: from OPENAI_API_KEY env var)",
    )
    parser.add_argument(
        "--companies-folder",
        default=None,
        help="Path to companies folder (default: ./companies relative to script)",
    )

    args = parser.parse_args()

    # Validate required parameters
    if not args.database_url:
        logger.error("DATABASE_URL not provided via --database-url or .env file")
        exit(1)

    if not args.openai_api_key:
        logger.error("OPENAI_API_KEY not provided via --openai-api-key or .env file")
        exit(1)

    process_ashby_companies(
        database_url=args.database_url,
        openai_api_key=args.openai_api_key,
        companies_folder=args.companies_folder,
    )
