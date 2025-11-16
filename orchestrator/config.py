"""
Pipeline Configuration

Defines available platforms, discovery methods, and pipeline steps.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class DiscoveryMethod(Enum):
    """Available discovery methods"""
    SEARXNG = "searxng"  # Self-hosted SearXNG (FREE unlimited)
    MANUAL = "manual"  # Skip automated discovery


class Platform(Enum):
    """Supported ATS platforms"""
    ASHBY = "ashby"
    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    WORKABLE = "workable"


class PipelineStep(Enum):
    """Pipeline execution steps"""
    DISCOVERY = "discovery"         # Step 1: Find companies using search APIs
    SCRAPING = "scraping"            # Step 2: Fetch jobs from each company via ATS APIs
    CSV_CONSOLIDATION = "csv"        # Step 3: Consolidate all jobs to simplified CSV
    DB_PROCESSING = "db_processing"  # Step 4: Send to database with embeddings
    EXPORT = "export"                # Step 5: Export final data


@dataclass
class PlatformConfig:
    """Configuration for a single platform"""
    name: str
    discovery_script: Optional[str] = None
    scraper_script: str = ""
    processor_script: Optional[str] = None
    companies_csv: str = ""
    output_dir: str = ""
    jobs_output_pattern: str = ""  # Pattern to find scraped jobs (e.g., "ashby/*.json")
    has_processing: bool = False


# Platform configurations
PLATFORM_CONFIGS = {
    Platform.ASHBY: PlatformConfig(
        name="ashby",
        scraper_script="ashby/main.py",
        processor_script="ashby/process_ashby.py",
        companies_csv="ashby/companies.csv",
        output_dir="ashby",
        jobs_output_pattern="ashby/companies/*.json",
        has_processing=True,
    ),
    Platform.GREENHOUSE: PlatformConfig(
        name="greenhouse",
        scraper_script="greenhouse/main.py",
        companies_csv="greenhouse/greenhouse_companies.csv",
        output_dir="greenhouse",
        jobs_output_pattern="greenhouse/companies/*.json",
        has_processing=False,
    ),
    Platform.LEVER: PlatformConfig(
        name="lever",
        scraper_script="lever/main.py",
        companies_csv="lever/lever_companies.csv",
        output_dir="lever",
        jobs_output_pattern="lever/companies/*.json",
        has_processing=False,
    ),
    Platform.WORKABLE: PlatformConfig(
        name="workable",
        scraper_script="workable/main.py",
        companies_csv="workable/workable_companies.csv",
        output_dir="workable",
        jobs_output_pattern="workable/companies/*.json",
        has_processing=False,
    ),
}


# Discovery method configurations
DISCOVERY_CONFIGS = {
    DiscoveryMethod.SEARXNG: {
        "script": "searxng_discovery.py",
        "requires_env": ["SEARXNG_URL"],
        "cost": "FREE (unlimited)",
        "speed": "Medium",
    },
    DiscoveryMethod.MANUAL: {
        "script": None,
        "requires_env": [],
        "cost": "FREE",
        "speed": "N/A",
    },
}


@dataclass
class PipelineConfig:
    """Full pipeline configuration"""
    # Which platforms to process
    platforms: List[Platform] = field(default_factory=lambda: list(Platform))

    # Which steps to execute
    steps: List[PipelineStep] = field(default_factory=lambda: list(PipelineStep))

    # Discovery method
    discovery_method: DiscoveryMethod = DiscoveryMethod.SEARXNG

    # Discovery parameters
    discovery_max_queries: int = 20
    discovery_pages: int = 3

    # Execution options
    parallel: bool = False
    verbose: bool = True

    # Error handling
    stop_on_error: bool = False
    retry_failed: bool = True
    max_retries: int = 3

    def should_run_step(self, step: PipelineStep) -> bool:
        """Check if a step should be executed"""
        return step in self.steps

    def get_platforms_to_process(self) -> List[Platform]:
        """Get list of platforms to process"""
        return self.platforms

    def validate(self) -> List[str]:
        """Validate configuration and return list of warnings/errors"""
        warnings = []

        # Check if any steps selected
        if not self.steps:
            warnings.append("No pipeline steps selected")

        # Check if any platforms selected
        if not self.platforms:
            warnings.append("No platforms selected")

        # Check discovery method requirements
        if PipelineStep.DISCOVERY in self.steps:
            method_config = DISCOVERY_CONFIGS[self.discovery_method]
            if method_config["script"] is None:
                warnings.append(f"Discovery method '{self.discovery_method.value}' requires manual intervention")

        return warnings
