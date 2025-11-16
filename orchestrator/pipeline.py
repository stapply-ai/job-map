"""
Pipeline Orchestrator

Coordinates the full job data aggregation workflow with configurable steps.

Usage:
    python orchestrator/pipeline.py --help
    python orchestrator/pipeline.py --all
    python orchestrator/pipeline.py --platforms ashby,greenhouse --skip-discovery
    python orchestrator/pipeline.py --discovery-method searxng --max-queries 30
"""

import subprocess
import sys
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
import argparse

from orchestrator.config import (
    PipelineConfig,
    PipelineStep,
    Platform,
    DiscoveryMethod,
    PLATFORM_CONFIGS,
    DISCOVERY_CONFIGS,
)


class PipelineOrchestrator:
    """Orchestrates the full job data aggregation pipeline"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.results: Dict[str, Dict] = {}
        self.start_time = None
        self.project_root = Path(__file__).parent.parent

    def log(self, message: str, level: str = "INFO"):
        """Log a message with timestamp"""
        if not self.config.verbose and level == "DEBUG":
            return

        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = {
            "INFO": "ℹ️ ",
            "SUCCESS": "✅",
            "ERROR": "❌",
            "WARNING": "⚠️ ",
            "DEBUG": "🔍",
        }.get(level, "")

        print(f"[{timestamp}] {prefix} {message}")

    def run_command(
        self, cmd: List[str], step_name: str, platform: Optional[str] = None
    ) -> bool:
        """
        Run a subprocess command with error handling

        Returns:
            True if successful, False otherwise
        """
        cmd_str = " ".join(cmd)
        self.log(f"Running: {cmd_str}", "DEBUG")

        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )

            if result.returncode == 0:
                self.log(f"✓ {step_name} completed successfully", "SUCCESS")
                if platform:
                    if platform not in self.results:
                        self.results[platform] = {}
                    self.results[platform][step_name] = "success"
                return True
            else:
                self.log(f"✗ {step_name} failed with exit code {result.returncode}", "ERROR")
                if result.stderr:
                    self.log(f"Error output: {result.stderr[:500]}", "ERROR")
                if platform:
                    if platform not in self.results:
                        self.results[platform] = {}
                    self.results[platform][step_name] = "failed"
                return False

        except subprocess.TimeoutExpired:
            self.log(f"✗ {step_name} timed out after 1 hour", "ERROR")
            return False
        except Exception as e:
            self.log(f"✗ {step_name} error: {e}", "ERROR")
            return False

    def run_discovery(self) -> bool:
        """
        Run company discovery step

        Returns:
            True if successful or skipped, False on error
        """
        if not self.config.should_run_step(PipelineStep.DISCOVERY):
            self.log("Skipping discovery step", "INFO")
            return True

        self.log("=" * 80)
        self.log(f"STEP 1: COMPANY DISCOVERY ({self.config.discovery_method.value})")
        self.log("=" * 80)

        method_config = DISCOVERY_CONFIGS[self.config.discovery_method]

        # Manual discovery - skip automated
        if method_config["script"] is None:
            self.log("Manual discovery selected - please update CSV files manually", "WARNING")
            return True

        # Build command
        script = method_config["script"]
        cmd = ["python", script, "--platform", "all"]

        # Add method-specific parameters
        if self.config.discovery_method in [
            DiscoveryMethod.ENHANCED,
            DiscoveryMethod.SEARXNG,
            DiscoveryMethod.FIRECRAWL,
            DiscoveryMethod.GOOGLE,
            DiscoveryMethod.OPTIMIZED,
        ]:
            cmd.extend(["--max-queries", str(self.config.discovery_max_queries)])

        if self.config.discovery_method in [DiscoveryMethod.ENHANCED, DiscoveryMethod.SEARXNG]:
            cmd.extend(["--pages", str(self.config.discovery_pages)])

        # Check required environment variables
        missing_env = [
            env for env in method_config["requires_env"] if not os.getenv(env)
        ]
        if missing_env:
            self.log(
                f"Missing required environment variables: {', '.join(missing_env)}",
                "ERROR",
            )
            self.log(f"Please add them to your .env file", "ERROR")
            return False

        # Display cost information
        self.log(f"Discovery method: {self.config.discovery_method.value}", "INFO")
        self.log(f"Cost: {method_config['cost']}", "INFO")
        self.log(f"Speed: {method_config['speed']}", "INFO")

        # Run discovery
        success = self.run_command(cmd, "discovery")

        if success:
            self.log(f"Discovery completed using {self.config.discovery_method.value}", "SUCCESS")
        elif not self.config.stop_on_error:
            self.log("Discovery failed, but continuing with existing companies", "WARNING")
            return True

        return success or not self.config.stop_on_error

    def run_scraping(self) -> bool:
        """
        Run job scraping step for all platforms

        Returns:
            True if all successful or non-critical errors, False on critical error
        """
        if not self.config.should_run_step(PipelineStep.SCRAPING):
            self.log("Skipping scraping step", "INFO")
            return True

        self.log("")
        self.log("=" * 80)
        self.log("STEP 2: JOB SCRAPING")
        self.log("=" * 80)

        platforms_to_scrape = self.config.get_platforms_to_process()

        if not platforms_to_scrape:
            self.log("No platforms selected for scraping", "WARNING")
            return True

        self.log(f"Platforms to scrape: {', '.join([p.value for p in platforms_to_scrape])}")

        all_success = True

        for platform_enum in platforms_to_scrape:
            platform_config = PLATFORM_CONFIGS[platform_enum]
            platform_name = platform_config.name

            self.log("")
            self.log(f"--- Scraping {platform_name.upper()} ---")

            # Check if companies CSV exists
            csv_path = self.project_root / platform_config.companies_csv
            if not csv_path.exists():
                self.log(f"Companies CSV not found: {platform_config.companies_csv}", "WARNING")
                self.log(f"Skipping {platform_name} scraping", "WARNING")
                continue

            # Run scraper
            scraper_script = platform_config.scraper_script
            cmd = ["python", scraper_script]

            success = self.run_command(cmd, f"scraping_{platform_name}", platform_name)

            if not success:
                all_success = False
                if self.config.stop_on_error:
                    self.log(f"Stopping pipeline due to {platform_name} scraping failure", "ERROR")
                    return False

            # Small delay between platforms
            if platform_enum != platforms_to_scrape[-1]:
                time.sleep(2)

        if all_success:
            self.log("All platform scraping completed successfully", "SUCCESS")
        else:
            self.log("Some platforms failed scraping, but continuing", "WARNING")

        return True

    def run_csv_consolidation(self) -> bool:
        """
        Run CSV consolidation step (simplified jobs CSV)

        Returns:
            True if successful or skipped, False on error
        """
        if not self.config.should_run_step(PipelineStep.CSV_CONSOLIDATION):
            self.log("Skipping CSV consolidation step", "INFO")
            return True

        self.log("")
        self.log("=" * 80)
        self.log("STEP 3: CSV CONSOLIDATION")
        self.log("=" * 80)

        platforms_to_consolidate = self.config.get_platforms_to_process()
        platform_names = [p.value for p in platforms_to_consolidate]

        self.log(f"Consolidating jobs from: {', '.join(platform_names)}")
        self.log("Creating simplified CSV (url, title, location, company)")

        # Run consolidation script
        cmd = [
            "python",
            "orchestrator/consolidate_jobs.py",
            "--platforms",
            ",".join(platform_names),
            "--output",
            "all_jobs.csv",
        ]

        success = self.run_command(cmd, "csv_consolidation")

        if success:
            self.log("CSV consolidation completed successfully", "SUCCESS")
        elif not self.config.stop_on_error:
            self.log("CSV consolidation failed, but continuing", "WARNING")
            return True

        return success or not self.config.stop_on_error

    def run_db_processing(self) -> bool:
        """
        Run database processing step (database + embeddings)

        Returns:
            True if successful or skipped, False on error
        """
        if not self.config.should_run_step(PipelineStep.DB_PROCESSING):
            self.log("Skipping database processing step", "INFO")
            return True

        self.log("")
        self.log("=" * 80)
        self.log("STEP 4: DATABASE PROCESSING")
        self.log("=" * 80)

        platforms_to_process = self.config.get_platforms_to_process()
        platforms_with_processing = [
            p for p in platforms_to_process if PLATFORM_CONFIGS[p].has_processing
        ]

        if not platforms_with_processing:
            self.log("No platforms have processing scripts available", "WARNING")
            self.log("Currently only Ashby has database + embeddings processing", "INFO")
            return True

        all_success = True

        for platform_enum in platforms_with_processing:
            platform_config = PLATFORM_CONFIGS[platform_enum]
            platform_name = platform_config.name

            self.log("")
            self.log(f"--- Processing {platform_name.upper()} ---")

            # Check required environment variables
            required_env = ["DATABASE_URL", "OPENAI_API_KEY"]
            missing_env = [env for env in required_env if not os.getenv(env)]
            if missing_env:
                self.log(
                    f"Missing required environment variables: {', '.join(missing_env)}",
                    "ERROR",
                )
                self.log(f"Skipping {platform_name} processing", "WARNING")
                continue

            # Run processor
            processor_script = platform_config.processor_script
            cmd = ["python", processor_script]

            success = self.run_command(cmd, f"processing_{platform_name}", platform_name)

            if not success:
                all_success = False
                if self.config.stop_on_error:
                    self.log(f"Stopping pipeline due to {platform_name} processing failure", "ERROR")
                    return False

        if all_success:
            self.log("Data processing completed successfully", "SUCCESS")
        else:
            self.log("Some processing failed, but continuing", "WARNING")

        return True

    def run_export(self) -> bool:
        """
        Run data export step

        Returns:
            True if successful, False on error
        """
        if not self.config.should_run_step(PipelineStep.EXPORT):
            self.log("Skipping export step", "INFO")
            return True

        self.log("")
        self.log("=" * 80)
        self.log("STEP 5: DATA EXPORT")
        self.log("=" * 80)

        # Currently, export is handled by individual platform scrapers
        # This step is a placeholder for future centralized export logic
        # (e.g., combining all platforms into a single jobs.csv)

        self.log("Export currently handled by individual platform scrapers", "INFO")
        self.log("Each platform outputs to its own directory", "INFO")

        return True

    def print_summary(self):
        """Print pipeline execution summary"""
        duration = time.time() - self.start_time
        minutes = int(duration // 60)
        seconds = int(duration % 60)

        self.log("")
        self.log("=" * 80)
        self.log("PIPELINE SUMMARY")
        self.log("=" * 80)

        self.log(f"Total duration: {minutes}m {seconds}s")
        self.log("")

        # Print results per platform
        if self.results:
            self.log("Results by platform:")
            for platform, steps in self.results.items():
                self.log(f"\n  {platform.upper()}:")
                for step, status in steps.items():
                    status_icon = "✅" if status == "success" else "❌"
                    self.log(f"    {status_icon} {step}: {status}")
        else:
            self.log("No results to display")

        self.log("")
        self.log("=" * 80)

    def run(self) -> bool:
        """
        Run the full pipeline

        Returns:
            True if pipeline completed successfully, False otherwise
        """
        self.start_time = time.time()

        self.log("=" * 80)
        self.log("JOB DATA AGGREGATION PIPELINE")
        self.log("=" * 80)

        # Validate configuration
        warnings = self.config.validate()
        if warnings:
            for warning in warnings:
                self.log(warning, "WARNING")

        # Display configuration
        self.log(f"Platforms: {', '.join([p.value for p in self.config.platforms])}")
        self.log(f"Steps: {', '.join([s.value for s in self.config.steps])}")
        self.log(f"Discovery method: {self.config.discovery_method.value}")
        self.log("")

        # Run pipeline steps
        steps = [
            (PipelineStep.DISCOVERY, self.run_discovery),
            (PipelineStep.SCRAPING, self.run_scraping),
            (PipelineStep.CSV_CONSOLIDATION, self.run_csv_consolidation),
            (PipelineStep.DB_PROCESSING, self.run_db_processing),
            (PipelineStep.EXPORT, self.run_export),
        ]

        for step_enum, step_func in steps:
            if not self.config.should_run_step(step_enum):
                continue

            success = step_func()

            if not success and self.config.stop_on_error:
                self.log(f"Pipeline stopped due to {step_enum.value} failure", "ERROR")
                self.print_summary()
                return False

        # Print summary
        self.print_summary()

        self.log("Pipeline execution completed!", "SUCCESS")
        return True


def parse_platforms(platform_str: str) -> List[Platform]:
    """Parse comma-separated platform string into Platform enums"""
    if platform_str.lower() == "all":
        return list(Platform)

    platforms = []
    for name in platform_str.split(","):
        name = name.strip().upper()
        try:
            platforms.append(Platform[name])
        except KeyError:
            raise ValueError(f"Unknown platform: {name}")

    return platforms


def main():
    """Main entry point for CLI"""
    parser = argparse.ArgumentParser(
        description="Job Data Aggregation Pipeline Orchestrator\n\nDefault behavior: Runs scraping + CSV consolidation + export (skips discovery and DB processing)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: scraping + CSV consolidation + export (no discovery, no DB)
  python orchestrator/pipeline.py

  # Run full pipeline including discovery and DB processing
  python orchestrator/pipeline.py --all

  # Run only discovery with Firecrawl
  python orchestrator/pipeline.py --discovery-only --discovery-method firecrawl --max-queries 20

  # Scraping + CSV only (skip export)
  python orchestrator/pipeline.py --skip-export

  # Full pipeline for specific platforms with optimized SERP discovery
  python orchestrator/pipeline.py --all --platforms ashby,greenhouse --discovery-method optimized --max-queries 10
        """,
    )

    # Platform selection
    parser.add_argument(
        "--platforms",
        type=str,
        default="all",
        help="Platforms to process (comma-separated: ashby,greenhouse,lever,workable or 'all')",
    )

    # Preset options
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all steps including discovery and DB processing (default discovery: searxng)",
    )

    parser.add_argument(
        "--discovery-only",
        action="store_true",
        help="Run only discovery step",
    )

    parser.add_argument(
        "--scraping-only",
        action="store_true",
        help="Run only scraping step",
    )

    # Step control
    parser.add_argument(
        "--skip-discovery",
        action="store_true",
        help="Skip company discovery step",
    )

    parser.add_argument(
        "--skip-scraping",
        action="store_true",
        help="Skip job scraping step",
    )

    parser.add_argument(
        "--skip-csv",
        action="store_true",
        help="Skip CSV consolidation step",
    )

    parser.add_argument(
        "--skip-db-processing",
        action="store_true",
        help="Skip database processing step (database + embeddings)",
    )

    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip export step",
    )

    # Discovery options
    parser.add_argument(
        "--discovery-method",
        type=str,
        default="searxng",
        choices=[m.value for m in DiscoveryMethod],
        help="Discovery method to use (default: searxng - FREE unlimited)",
    )

    parser.add_argument(
        "--max-queries",
        type=int,
        default=20,
        help="Maximum queries per platform for discovery (default: 20)",
    )

    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Pages per query for discovery (default: 3)",
    )

    # Execution options
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop pipeline execution on first error (default: continue)",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce output verbosity",
    )

    args = parser.parse_args()

    # Build configuration
    config = PipelineConfig()

    # Parse platforms
    try:
        config.platforms = parse_platforms(args.platforms)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Determine which steps to run
    if args.all:
        config.steps = list(PipelineStep)
    elif args.discovery_only:
        config.steps = [PipelineStep.DISCOVERY]
    elif args.scraping_only:
        config.steps = [PipelineStep.SCRAPING]
    else:
        # Default: scraping + CSV + export (skip discovery and DB processing)
        # Use --all to run all steps including discovery and DB processing
        config.steps = []
        if not args.skip_scraping:
            config.steps.append(PipelineStep.SCRAPING)
        if not args.skip_csv:
            config.steps.append(PipelineStep.CSV_CONSOLIDATION)
        if not args.skip_export:
            config.steps.append(PipelineStep.EXPORT)

    # Discovery configuration
    try:
        config.discovery_method = DiscoveryMethod(args.discovery_method)
    except ValueError:
        print(f"Error: Invalid discovery method: {args.discovery_method}")
        sys.exit(1)

    config.discovery_max_queries = args.max_queries
    config.discovery_pages = args.pages

    # Execution options
    config.stop_on_error = args.stop_on_error
    config.verbose = not args.quiet

    # Run pipeline
    orchestrator = PipelineOrchestrator(config)
    success = orchestrator.run()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
