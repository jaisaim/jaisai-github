# =============================================================================
# FILE: jpetstore_perf_test.py
# PROJECT: JPetStore Web Application - Performance Testing
# DESCRIPTION: 3 Simple Scenarios tested using 6-Stage Intelligent AI Agents
#
# SCENARIO 1 (Happy Path): Browse catalog -> View product -> Add to cart -> Checkout
# SCENARIO 2 (User Login): Load login page -> Submit credentials -> View account
# SCENARIO 3 (Catalog Browse): Browse categories -> View items -> Search products
#
# 6-STAGE AGENT PIPELINE:
#   Stage 1: PlanningAgent    - Reads config, builds test plan
#   Stage 2: DiscoveryAgent   - Checks if JPetStore URLs are reachable
#   Stage 3: WorkloadAgent    - Defines the 3 scenarios with steps
#   Stage 4: ExecutionAgent   - Runs virtual users against each scenario
#   Stage 5: AnalysisAgent    - Computes response time, errors, throughput
#   Stage 6: ReportingAgent   - Prints summary and saves JSON report
#
# HOW TO RUN:
#   1. Start JPetStore locally: http://localhost:8080/jpetstore
#   2. Run: python jpetstore_perf_test.py
#
# NOTE: Uses Python standard library only. No pip install needed.
# =============================================================================

import time          # Used to measure request response times and add think-time delays
import json          # Used to save the final test report as a JSON file
import logging       # Used to print timestamped stage-by-stage log messages
import threading     # Used to run multiple virtual users at the same time (concurrency)
import queue         # Used to safely collect results from all virtual user threads
import statistics    # Used to calculate mean, median, and percentile response times
from datetime import datetime          # Used to timestamp the report
from urllib.request import urlopen, Request  # Used to send HTTP GET requests
from urllib.error import URLError, HTTPError # Used to catch HTTP and network errors

# =============================================================================
# LOGGING SETUP
# Creates a logger that prints timestamps + stage messages to the console
# =============================================================================
logging.basicConfig(
    level=logging.INFO,                             # Show INFO level and above
    format='%(asctime)s [%(levelname)s] %(message)s' # Format: time [level] message
)
log = logging.getLogger("JPetStore-PerfTest")       # Named logger for this script

# =============================================================================
# JPETSTORE CONFIGURATION
# Change target_url to match where your JPetStore is running
# The 3 scenarios below represent real user journeys through the application
# =============================================================================
CONFIG = {
    # Base URL of the JPetStore application under test
    "target_url": "https://petstore.octoperf.com/actions",

    # Scenario 1: Happy Path - A user browses, selects, and purchases a pet
    "scenario_happy_path": {
        "name": "Happy Path - Browse and Buy",          # Human-readable scenario name
        "steps": [                                       # Ordered list of HTTP steps
            {"label": "Home Page",     "path": "/Catalog.action"},          # Step 1: Load home
            {"label": "Fish Category", "path": "/Catalog.action?viewCategory=&categoryId=FISH"},  # Step 2: Browse fish
            {"label": "View Product",  "path": "/Catalog.action?viewProduct=&productId=FI-SW-01"}, # Step 3: See goldfish
            {"label": "View Item",     "path": "/Catalog.action?viewItem=&itemId=EST-1"},           # Step 4: See item detail
            {"label": "Add To Cart",   "path": "/Cart.action?addItemToCart=&workingItemId=EST-1"},  # Step 5: Add to cart
            {"label": "View Cart",     "path": "/Cart.action?viewCart="},                           # Step 6: See cart
        ]
    },

    # Scenario 2: User Login - A user navigates to login and signs in
    "scenario_login": {
        "name": "User Login Flow",                       # Human-readable scenario name
        "steps": [
            {"label": "Home Page",    "path": "/Catalog.action"},            # Step 1: Load home page
            {"label": "Sign-In Page", "path": "/Account.action?signonForm="}, # Step 2: Open login form
            {"label": "My Account",   "path": "/Account.action?viewAccount="}, # Step 3: View account page
        ]
    },

    # Scenario 3: Catalog Browse - A user explores all pet categories
    "scenario_catalog": {
        "name": "Catalog Browse",                        # Human-readable scenario name
        "steps": [
            {"label": "Home Page",       "path": "/Catalog.action"},                                        # Step 1: Home
            {"label": "Dogs Category",   "path": "/Catalog.action?viewCategory=&categoryId=DOGS"},           # Step 2: Dogs
            {"label": "Cats Category",   "path": "/Catalog.action?viewCategory=&categoryId=CATS"},           # Step 3: Cats
            {"label": "Reptiles",        "path": "/Catalog.action?viewCategory=&categoryId=REPTILES"},       # Step 4: Reptiles
            {"label": "Birds",           "path": "/Catalog.action?viewCategory=&categoryId=BIRDS"},          # Step 5: Birds
            {"label": "Search Goldfish", "path": "/Catalog.action?searchProducts=&keyword=goldfish"},        # Step 6: Search
        ]
    },

    "virtual_users": 3,          # Number of concurrent virtual users per scenario
    "think_time_sec": 1,         # Pause (seconds) between each step to simulate real user
    "timeout_sec": 10,           # Max seconds to wait for a single HTTP response
    "test_duration_sec": 20,     # Total time (seconds) each scenario runs

    # Thresholds: used by Analysis Agent to PASS or FAIL the test
    "thresholds": {
        "max_avg_response_ms": 3000,   # Average response must be under 3 seconds
        "max_error_rate_pct": 10.0,    # Error rate must be under 10%
        "min_throughput_rps": 0.5,     # Must process at least 0.5 requests/second
    }
}

# =============================================================================
# STAGE 1: PLANNING AGENT
# Reads the CONFIG, validates it, and builds a structured test plan.
# The plan is passed to every downstream agent.
# =============================================================================
class PlanningAgent:
    """
    Stage 1 - Planning Agent
    Goal: Validate configuration and produce a structured test plan.
    Input: CONFIG dictionary
    Output: plan dictionary used by all other agents
    """

    def __init__(self, config):
        self.config = config    # Store the global config for validation and plan building

    def validate(self):
        """
        Check that all required config keys are present.
        Raises ValueError if anything is missing or invalid.
        """
        assert self.config.get("target_url"),           "Missing target_url in CONFIG"
        assert self.config.get("scenario_happy_path"),  "Missing scenario_happy_path"
        assert self.config.get("scenario_login"),        "Missing scenario_login"
        assert self.config.get("scenario_catalog"),      "Missing scenario_catalog"
        assert self.config["virtual_users"] >= 1,        "virtual_users must be >= 1"
        assert self.config["test_duration_sec"] >= 5,    "test_duration_sec must be >= 5"

    def run(self):
        """
        Validate the config and build the final test plan dictionary.
        Returns the plan dict which is passed to all downstream agents.
        """
        log.info("=== STAGE 1: PLANNING AGENT STARTED ===")
        self.validate()                             # Make sure config is correct first

        # Build the plan dictionary from config values
        plan = {
            "target_url":       self.config["target_url"],          # Base JPetStore URL
            "scenarios": [                                           # List of 3 scenarios
                self.config["scenario_happy_path"],                  # Scenario 1
                self.config["scenario_login"],                       # Scenario 2
                self.config["scenario_catalog"],                     # Scenario 3
            ],
            "virtual_users":    self.config["virtual_users"],        # Concurrent VUs
            "think_time_sec":   self.config["think_time_sec"],       # Think time per step
            "timeout_sec":      self.config["timeout_sec"],          # HTTP timeout
            "test_duration_sec":self.config["test_duration_sec"],    # Total test time
            "thresholds":       self.config["thresholds"],           # Pass/fail limits
            "created_at":       datetime.utcnow().isoformat(),       # Timestamp of plan
        }

        # Log a summary of the plan for visibility
        log.info(f"Target     : {plan['target_url']}")
        log.info(f"Scenarios  : {[s['name'] for s in plan['scenarios']]}")
        log.info(f"VUsers     : {plan['virtual_users']} per scenario")
        log.info(f"Duration   : {plan['test_duration_sec']}s per scenario")
        log.info("=== STAGE 1: PLANNING AGENT COMPLETED ===")
        return plan                                  # Pass plan to next agent


# =============================================================================
# STAGE 2: DISCOVERY AGENT
# Sends a single probe request to each unique URL in all scenarios.
# Confirms JPetStore is UP before the load test starts.
# =============================================================================
class DiscoveryAgent:
    """
    Stage 2 - Discovery Agent
    Goal: Verify JPetStore is reachable on all scenario endpoints.
    Input: plan dictionary from PlanningAgent
    Output: list of probe results (reachable/not-reachable per URL)
    """

    def __init__(self, plan):
        self.plan = plan        # Store the test plan to get base URL and scenarios

    def probe(self, url, timeout):
        """
        Send one HTTP GET to a URL and measure the response time.
        Returns a dict with reachability status and response details.
        """
        result = {
            "url":             url,     # Full URL that was probed
            "reachable":       False,   # Default: not reachable
            "status_code":     None,    # HTTP response code (200, 404, etc.)
            "response_time_ms":None,    # How long the request took in ms
            "error":           None     # Error message if request failed
        }
        try:
            req   = Request(url, headers={"User-Agent": "JPetStore-PerfBot/1.0"})
            start = time.time()                         # Start timing the request
            with urlopen(req, timeout=timeout) as resp: # Make the HTTP GET call
                result["status_code"]      = resp.getcode()                    # Save HTTP status
                result["response_time_ms"] = round((time.time() - start)*1000, 1) # Save latency
                result["reachable"]        = True                              # Mark reachable
        except HTTPError as e:
            result["status_code"] = e.code              # Capture HTTP error code (e.g. 403)
            result["error"]       = f"HTTP {e.code}"    # Record the error
        except URLError as e:
            result["error"] = str(e)                    # Capture connection-level error
        except Exception as e:
            result["error"] = str(e)                    # Catch any unexpected errors
        return result                                   # Return probe result dict

    def run(self):
        """
        Probe the home page + first step of each scenario.
        Warns if any URL is not reachable so the tester can fix before running.
        Returns list of all probe result dicts.
        """
        log.info("=== STAGE 2: DISCOVERY AGENT STARTED ===")
        base    = self.plan["target_url"]               # Get base URL from plan
        timeout = self.plan["timeout_sec"]              # Get timeout from plan
        probed  = {}                                    # Dict to avoid duplicate probes

        for scenario in self.plan["scenarios"]:         # Iterate over each scenario
            for step in scenario["steps"]:              # Iterate over each step
                url = base + step["path"]               # Construct full URL
                if url not in probed:                   # Skip if already probed this URL
                    result = self.probe(url, timeout)   # Send probe request
                    probed[url] = result                # Store result by URL key
                    status = result["status_code"] or "ERR"  # Display code or ERR
                    rt     = result["response_time_ms"] or "N/A"
                    flag   = "OK" if result["reachable"] else "UNREACHABLE"
                    log.info(f"[{flag}] {step['label']}: {status} in {rt}ms -> {url}")

        discovery_results = list(probed.values())       # Convert dict to list
        reachable_count   = sum(1 for r in discovery_results if r["reachable"])
        log.info(f"Discovery: {reachable_count}/{len(discovery_results)} URLs reachable")
        log.info("=== STAGE 2: DISCOVERY AGENT COMPLETED ===")
        return discovery_results                        # Pass results to next agent


# =============================================================================
# STAGE 3: WORKLOAD DESIGN AGENT
# Takes the 3 scenarios and defines how virtual users will execute them.
# Adds think-time, timeout, and base URL to each scenario step.
# =============================================================================
class WorkloadAgent:
    """
    Stage 3 - Workload Design Agent
    Goal: Prepare executable workload definitions for the Execution Agent.
    Input: plan dictionary from PlanningAgent
    Output: list of workload dicts (one per scenario)
    """

    def __init__(self, plan):
        self.plan = plan        # Store plan for building workloads

    def build_workload(self, scenario):
        """
        Build a workload dict from a scenario definition.
        Attaches base_url, timeout and think_time to each step.
        Returns a workload dict ready for execution.
        """
        base    = self.plan["target_url"]           # Base JPetStore URL
        timeout = self.plan["timeout_sec"]          # Timeout per HTTP step
        think   = self.plan["think_time_sec"]       # Think time between steps

        # Build list of executable steps with full URL and timing info
        steps = []
        for step in scenario["steps"]:              # Iterate over scenario steps
            steps.append({
                "label":      step["label"],        # Step name (e.g. "Home Page")
                "url":        base + step["path"],  # Full URL to request
                "timeout":    timeout,              # Max wait for this step
                "think_time": think,                # Pause after this step
            })

        return {
            "scenario_name":    scenario["name"],           # Scenario name label
            "steps":            steps,                      # List of executable steps
            "virtual_users":    self.plan["virtual_users"], # How many concurrent VUs
            "test_duration_sec":self.plan["test_duration_sec"], # How long to run
        }

    def run(self):
        """
        Build workloads for all 3 scenarios.
        Logs each scenario and step for visibility.
        Returns list of 3 workload dicts.
        """
        log.info("=== STAGE 3: WORKLOAD DESIGN AGENT STARTED ===")
        workloads = []                                   # Will hold all 3 workloads

        for scenario in self.plan["scenarios"]:          # Loop through each scenario
            workload = self.build_workload(scenario)     # Build workload definition
            workloads.append(workload)                   # Add to workload list
            log.info(f"Workload ready: '{workload['scenario_name']}' "
                     f"({len(workload['steps'])} steps, "
                     f"{workload['virtual_users']} VUs)")
            for step in workload["steps"]:               # Log each step URL
                log.info(f"   Step: {step['label']} -> {step['url']}")

        log.info("=== STAGE 3: WORKLOAD DESIGN AGENT COMPLETED ===")
        return workloads                                 # Pass workloads to Execution Agent


# =============================================================================
# STAGE 4: EXECUTION AGENT
# Runs each of the 3 workloads using multiple virtual user threads.
# Each virtual user loops through all steps in the scenario repeatedly.
# Results are collected into a shared thread-safe queue.
# =============================================================================
class ExecutionAgent:
    """
    Stage 4 - Execution Agent
    Goal: Execute all 3 workloads with concurrent virtual users.
    Input: list of workload dicts from WorkloadAgent
    Output: dict of raw results keyed by scenario name
    """

    def __init__(self, workloads):
        self.workloads = workloads  # Store list of 3 workloads to execute

    def run_virtual_user(self, vu_id, workload, results_q, stop_event):
        """
        Simulates one virtual user executing all steps in a scenario.
        Repeats until stop_event is set (test duration reached).
        Pushes each step result into results_q (shared queue).

        Args:
            vu_id:      Integer ID of this virtual user (e.g. VU-1)
            workload:   Workload dict with steps, timing, scenario name
            results_q:  Thread-safe queue to push result dicts into
            stop_event: threading.Event that signals when test should end
        """
        while not stop_event.is_set():              # Keep looping until time is up
            for step in workload["steps"]:          # Execute each step in order
                if stop_event.is_set():             # Check again before each step
                    break                           # Stop immediately if signalled

                result = {
                    "vu_id":           vu_id,                    # Which VU ran this step
                    "scenario":        workload["scenario_name"], # Which scenario
                    "step":            step["label"],             # Which step label
                    "url":             step["url"],               # Which URL was called
                    "timestamp":       time.time(),               # When step was started
                    "response_time_ms":None,                      # Will be filled below
                    "status_code":     None,                      # HTTP status
                    "success":         False,                     # Default: failed
                    "error":           None                       # Error if any
                }

                start = time.time()                 # Record start time of HTTP call
                try:
                    req = Request(                  # Build HTTP request object
                        step["url"],
                        headers={"User-Agent": f"VU-{vu_id}/JPetStore-PerfTest"}
                    )
                    with urlopen(req, timeout=step["timeout"]) as resp:   # Execute request
                        result["status_code"]      = resp.getcode()        # Save status code
                        result["response_time_ms"] = round((time.time()-start)*1000, 1)
                        result["success"]          = True                  # Mark as success
                except HTTPError as e:
                    result["status_code"]      = e.code              # HTTP error code
                    result["response_time_ms"] = round((time.time()-start)*1000, 1)
                    result["error"]            = f"HTTP {e.code}"    # Error description
                except Exception as e:
                    result["response_time_ms"] = round((time.time()-start)*1000, 1)
                    result["error"]            = str(e)              # General error

                results_q.put(result)               # Push result into shared queue
                time.sleep(step["think_time"])      # Pause before next step

    def run_workload(self, workload):
        """
        Runs one scenario workload with N virtual users in parallel threads.
        Waits for the test_duration_sec, then signals all VUs to stop.
        Returns a list of all raw step results for this scenario.

        Args:
            workload: A single workload dict (one of the 3 scenarios)
        Returns:
            List of raw result dicts for this scenario
        """
        results_q  = queue.Queue()          # Thread-safe queue for collecting results
        stop_event = threading.Event()     # Event to signal VUs to stop
        threads    = []                    # Track all VU threads

        # Start one thread per virtual user
        for i in range(workload["virtual_users"]):      # Loop through VU count
            t = threading.Thread(
                target=self.run_virtual_user,           # Target function for this VU
                args=(i+1, workload, results_q, stop_event),  # Pass VU args
                daemon=True                             # Daemon so it exits when main ends
            )
            threads.append(t)                           # Track the thread
            t.start()                                   # Start the VU thread
            log.info(f"   VU-{i+1} started for '{workload['scenario_name']}'")

        # Wait for the full test duration
        log.info(f"   Running '{workload['scenario_name']}' for {workload['test_duration_sec']}s...")
        time.sleep(workload["test_duration_sec"])        # Sleep = test duration
        stop_event.set()                                # Signal all VUs to stop

        for t in threads:                               # Wait for all VUs to finish
            t.join(timeout=5)                           # Max 5s wait per thread

        raw = []                                        # Collect results from queue
        while not results_q.empty():                    # Drain the queue
            raw.append(results_q.get())                 # Append each result

        log.info(f"   '{workload['scenario_name']}': {len(raw)} requests collected")
        return raw                                      # Return raw results list

    def run(self):
        """
        Runs all 3 workloads one after another (sequential scenarios).
        Returns a dict mapping scenario_name -> list of raw results.
        """
        log.info("=== STAGE 4: EXECUTION AGENT STARTED ===")
        all_results = {}                                # Dict: scenario -> results list

        for workload in self.workloads:                 # Run each of the 3 scenarios
            log.info(f"Executing scenario: '{workload['scenario_name']}'")
            results = self.run_workload(workload)        # Run this workload
            all_results[workload["scenario_name"]] = results  # Store by scenario name

        log.info("=== STAGE 4: EXECUTION AGENT COMPLETED ===")
        return all_results                              # Pass all results to Analysis Agent


# =============================================================================
# STAGE 5: ANALYSIS AGENT
# Processes the raw results for each scenario and computes KPIs.
# Evaluates each scenario against the configured thresholds.
# =============================================================================
class AnalysisAgent:
    """
    Stage 5 - Analysis Agent
    Goal: Compute performance KPIs per scenario and evaluate pass/fail.
    Input: all_results dict from ExecutionAgent, thresholds from plan
    Output: analysis dict with metrics and PASS/FAIL per scenario
    """

    def __init__(self, plan, all_results):
        self.plan        = plan         # Plan contains thresholds
        self.all_results = all_results  # Raw results dict keyed by scenario name

    def compute_metrics(self, results):
        """
        Compute KPIs from a list of raw step results for one scenario.
        Returns a metrics dict with response times, error rate, throughput.

        Args:
            results: List of raw result dicts for one scenario
        Returns:
            Dict with computed KPI values
        """
        total      = len(results)                               # Total steps executed
        successful = [r for r in results if r["success"]]       # Successful steps
        failed     = [r for r in results if not r["success"]]   # Failed steps

        # Compute error rate as a percentage
        error_rate = round(len(failed) / total * 100, 2) if total > 0 else 0.0

        # Collect all response times from successful steps
        times = [r["response_time_ms"] for r in successful if r["response_time_ms"]]

        # Compute response time statistics
        if times:                                               # Guard: only if we have data
            sorted_t = sorted(times)                            # Sort for percentile calc
            n        = len(sorted_t)
            avg_ms   = round(statistics.mean(times), 1)         # Average response time
            med_ms   = round(statistics.median(times), 1)       # Median response time
            p95_ms   = sorted_t[min(int(n*0.95), n-1)]          # 95th percentile
            min_ms   = sorted_t[0]                              # Fastest step
            max_ms   = sorted_t[-1]                             # Slowest step
        else:
            avg_ms = med_ms = p95_ms = min_ms = max_ms = 0     # All zero if no data

        # Compute throughput: total requests / total test duration
        duration_sec = self.plan["test_duration_sec"]           # Get test duration
        rps          = round(total / duration_sec, 2) if duration_sec > 0 else 0

        return {
            "total_steps":      total,          # Total step executions across all VUs
            "successful_steps": len(successful), # Steps that returned success=True
            "failed_steps":     len(failed),     # Steps that returned success=False
            "error_rate_pct":   error_rate,      # % of steps that failed
            "throughput_rps":   rps,             # Requests per second overall
            "avg_response_ms":  avg_ms,          # Average latency in ms
            "median_ms":        med_ms,          # Median latency in ms
            "p95_ms":           p95_ms,          # 95th percentile latency in ms
            "min_ms":           min_ms,          # Minimum latency in ms
            "max_ms":           max_ms,          # Maximum latency in ms
        }

    def evaluate(self, metrics):
        """
        Compare metrics against thresholds to produce PASS or FAIL per check.
        Returns dict of check_name -> PASS/FAIL and an overall result.

        Args:
            metrics: Metrics dict from compute_metrics()
        Returns:
            Dict with individual threshold results and overall PASS/FAIL
        """
        t = self.plan["thresholds"]                             # Shorthand for thresholds
        checks = {
            "avg_response_time": "PASS" if metrics["avg_response_ms"] <= t["max_avg_response_ms"] else "FAIL",
            "error_rate":        "PASS" if metrics["error_rate_pct"]   <= t["max_error_rate_pct"]  else "FAIL",
            "throughput":        "PASS" if metrics["throughput_rps"]   >= t["min_throughput_rps"]   else "FAIL",
        }
        # Overall is PASS only if every individual check passed
        overall = "PASS" if all(v == "PASS" for v in checks.values()) else "FAIL"
        checks["overall"] = overall                             # Add overall to dict
        return checks                                           # Return all check results

    def run(self):
        """
        Compute metrics and threshold evaluation for all 3 scenarios.
        Logs a summary per scenario.
        Returns analysis dict keyed by scenario name.
        """
        log.info("=== STAGE 5: ANALYSIS AGENT STARTED ===")
        analysis = {}                                           # Results dict

        for scenario_name, results in self.all_results.items():  # Per scenario
            metrics   = self.compute_metrics(results)             # Compute KPIs
            threshold = self.evaluate(metrics)                    # Evaluate vs limits
            analysis[scenario_name] = {
                "metrics":    metrics,    # All computed KPI values
                "thresholds": threshold,  # PASS/FAIL per check + overall
            }
            log.info(f"Scenario: '{scenario_name}' -> Overall: {threshold['overall']}")
            log.info(f"  Avg={metrics['avg_response_ms']}ms | "
                     f"P95={metrics['p95_ms']}ms | "
                     f"ErrRate={metrics['error_rate_pct']}% | "
                     f"RPS={metrics['throughput_rps']}")

        log.info("=== STAGE 5: ANALYSIS AGENT COMPLETED ===")
        return analysis                                          # Pass to Reporting Agent


# =============================================================================
# STAGE 6: REPORTING AGENT
# Formats the analysis results into a clean console report and JSON file.
# Also generates simple AI recommendations based on metric values.
# =============================================================================
class ReportingAgent:
    """
    Stage 6 - Reporting Agent
    Goal: Generate human-readable console report + machine-readable JSON.
    Input: plan, analysis dict from AnalysisAgent
    Output: printed report + perf_report_jpetstore.json file
    """

    def __init__(self, plan, analysis):
        self.plan     = plan      # Test plan for config details
        self.analysis = analysis  # Analysis results for all 3 scenarios

    def recommend(self, scenario_name, metrics, thresholds):
        """
        Generate simple AI-style recommendations based on metric values.
        Returns a list of recommendation strings.

        Args:
            scenario_name: Name of the scenario (for context)
            metrics:       KPI dict from AnalysisAgent
            thresholds:    PASS/FAIL dict from AnalysisAgent
        Returns:
            List of recommendation strings
        """
        recs = []                                               # Empty recommendations list

        # Check if average response time is too slow
        if thresholds.get("avg_response_time") == "FAIL":
            recs.append(
                f"SLOW RESPONSE: '{scenario_name}' avg response is {metrics['avg_response_ms']}ms. "
                "Consider adding server-side caching or a CDN in front of JPetStore."
            )

        # Check if error rate is too high
        if thresholds.get("error_rate") == "FAIL":
            recs.append(
                f"HIGH ERRORS: '{scenario_name}' has {metrics['error_rate_pct']}% error rate. "
                "Check if JPetStore is running and session handling is correct."
            )

        # Check if throughput is too low
        if thresholds.get("throughput") == "FAIL":
            recs.append(
                f"LOW THROUGHPUT: '{scenario_name}' only at {metrics['throughput_rps']} req/s. "
                "Try increasing server resources or connection pool size."
            )

        # Check P95 latency even if average passed
        if metrics.get("p95_ms", 0) > 4000:
            recs.append(
                f"HIGH P95: '{scenario_name}' P95 latency is {metrics['p95_ms']}ms. "
                "Some users experience very slow pages. Investigate slow endpoints."
            )

        # If everything looks good
        if not recs:
            recs.append(
                f"'{scenario_name}' PASSED all thresholds. "
                "Application is handling load well for this scenario."
            )

        return recs                                             # Return recommendation list

    def print_report(self, full_report):
        """
        Print a formatted performance summary to the console.
        Shows metrics and threshold results for all 3 scenarios.

        Args:
            full_report: The complete report dict built in run()
        """
        print("\n" + "="*65)
        print("  JPETSTORE PERFORMANCE TEST REPORT")
        print("  6-Stage Intelligent AI Agent Pipeline")
        print("="*65)
        print(f"  Target  : {self.plan['target_url']}")
        print(f"  Date    : {full_report['generated_at']}")
        print(f"  VUsers  : {self.plan['virtual_users']} per scenario")
        print(f"  Duration: {self.plan['test_duration_sec']}s per scenario")
        print("="*65)

        for s_name, data in full_report["scenarios"].items():   # Per scenario section
            m   = data["metrics"]                               # Shorthand for metrics
            thr = data["thresholds"]                            # Shorthand for thresholds
            print(f"\n  SCENARIO: {s_name}")
            print("-"*65)
            print(f"  Total Steps      : {m['total_steps']}")
            print(f"  Successful       : {m['successful_steps']}")
            print(f"  Failed           : {m['failed_steps']}")
            print(f"  Error Rate       : {m['error_rate_pct']}%")
            print(f"  Throughput       : {m['throughput_rps']} req/s")
            print(f"  Avg Response     : {m['avg_response_ms']} ms")
            print(f"  Median Response  : {m['median_ms']} ms")
            print(f"  P95 Response     : {m['p95_ms']} ms")
            print(f"  Min Response     : {m['min_ms']} ms")
            print(f"  Max Response     : {m['max_ms']} ms")
            print(f"  Avg RT Check     : {thr['avg_response_time']}")
            print(f"  Error Rate Check : {thr['error_rate']}")
            print(f"  Throughput Check : {thr['throughput']}")
            print(f"  ** OVERALL       : {thr['overall']} **")
            print("  Recommendations:")
            for rec in data["recommendations"]:                 # Print each recommendation
                print(f"    -> {rec}")

        print("\n" + "="*65)
        print(f"  Report saved to: perf_report_jpetstore.json")
        print("="*65 + "\n")

    def run(self):
        """
        Build the full report dict, print it, and save it as JSON.
        Returns the complete report dict.
        """
        log.info("=== STAGE 6: REPORTING AGENT STARTED ===")

        # Build the full report structure
        full_report = {
            "title":        "JPetStore Performance Test Report",  # Report title
            "generated_at": datetime.utcnow().isoformat(),        # Timestamp
            "target_url":   self.plan["target_url"],              # Tested URL
            "config": {                                            # Key settings summary
                "virtual_users":     self.plan["virtual_users"],
                "test_duration_sec": self.plan["test_duration_sec"],
                "think_time_sec":    self.plan["think_time_sec"],
                "thresholds":        self.plan["thresholds"],
            },
            "scenarios": {}                                        # Will hold per-scenario data
        }

        # Add recommendations to each scenario and merge into report
        for s_name, data in self.analysis.items():               # Iterate scenarios
            recs = self.recommend(                               # Generate AI recommendations
                s_name,
                data["metrics"],
                data["thresholds"]
            )
            full_report["scenarios"][s_name] = {                 # Add to report
                "metrics":         data["metrics"],              # KPI metrics
                "thresholds":      data["thresholds"],           # PASS/FAIL checks
                "recommendations": recs,                         # AI recommendations
            }

        self.print_report(full_report)                           # Print to console

        # Save full report as JSON file
        with open("perf_report_jpetstore.json", "w") as f:      # Open file for writing
            json.dump(full_report, f, indent=2)                  # Write pretty JSON

        log.info("=== STAGE 6: REPORTING AGENT COMPLETED ===")
        return full_report                                        # Return complete report


# =============================================================================
# MAIN ORCHESTRATOR
# Chains all 6 agents in sequence:
# Plan -> Discover -> Design -> Execute -> Analyze -> Report
# =============================================================================
def run_jpetstore_perf_test():
    """
    Orchestrates the full 6-stage performance test pipeline for JPetStore.
    Each stage receives the output of the previous stage as its input.
    Returns the final report dict.
    """

    # ---------- STAGE 1: Planning Agent ----------
    planner    = PlanningAgent(CONFIG)           # Create planner with global config
    plan       = planner.run()                   # Build and validate the test plan

    # ---------- STAGE 2: Discovery Agent ----------
    discoverer = DiscoveryAgent(plan)            # Create discoverer with the plan
    _discovery = discoverer.run()                # Probe all URLs (results logged only)

    # ---------- STAGE 3: Workload Design Agent ----------
    designer   = WorkloadAgent(plan)             # Create workload designer with plan
    workloads  = designer.run()                  # Build 3 executable workload dicts

    # ---------- STAGE 4: Execution Agent ----------
    executor   = ExecutionAgent(workloads)       # Create executor with 3 workloads
    all_results= executor.run()                  # Run all 3 scenarios, collect results

    # ---------- STAGE 5: Analysis Agent ----------
    analyzer   = AnalysisAgent(plan, all_results) # Create analyzer with plan + results
    analysis   = analyzer.run()                   # Compute KPIs and PASS/FAIL per scenario

    # ---------- STAGE 6: Reporting Agent ----------
    reporter   = ReportingAgent(plan, analysis)  # Create reporter with plan + analysis
    report     = reporter.run()                  # Print report and save JSON

    return report                                # Return the full final report


# =============================================================================
# ENTRY POINT
# Run the test when this script is executed directly
# =============================================================================
if __name__ == "__main__":
    # Print startup banner
    print("\n" + "#"*65)
    print("  JPetStore Performance Test - 6-Stage AI Agent Pipeline")
    print("  Scenarios: Happy Path | User Login | Catalog Browse")
    print("#"*65 + "\n")

    # Run the full test pipeline
    final = run_jpetstore_perf_test()

    # Print the overall summary line
    results_summary = {
        name: data["thresholds"]["overall"]
        for name, data in final["scenarios"].items()
    }
    print("FINAL RESULTS:", results_summary)
