# =============================================================================
# FILE: perf_test_agent.py
# DESCRIPTION: Web Application Performance Testing using 6-Stage Intelligent AI Agents
# ALGORITHM: Plan -> Discover -> Design -> Execute -> Analyze -> Report
# AUTHOR: jaisaim
# BRANCH: perftest-webapplication
# =============================================================================

# Import standard library modules for HTTP requests and timing
import time                    # Used to measure elapsed time during load tests
import statistics              # Used to compute mean, median, stdev of response times
import json                    # Used to serialize/deserialize JSON data for reports
import logging                 # Used to log messages at each agent stage
import random                  # Used to simulate randomized user behavior
import threading               # Used to run concurrent virtual users in parallel
import queue                   # Used to collect results safely from multiple threads
from datetime import datetime  # Used to timestamp reports and log events
from urllib.request import urlopen, Request  # Used to make HTTP GET/POST requests
from urllib.error import URLError, HTTPError  # Used to handle HTTP/network errors

# =============================================================================
# LOGGING SETUP
# Configure a global logger so every agent stage prints timestamped messages
# =============================================================================
logging.basicConfig(
    level=logging.INFO,                            # Set minimum log level to INFO
    format='%(asctime)s [%(levelname)s] %(message)s'  # Include timestamp in logs
)
logger = logging.getLogger("PerfTestAgent")        # Create a named logger instance

# =============================================================================
# CONFIGURATION: Central config dict used by all 6 agents
# Modify these values to point at your target web application
# =============================================================================
CONFIG = {
    "target_url": "https://httpbin.org",           # Base URL of the web app under test
    "endpoints": [                                  # List of API/page endpoints to test
        "/get",                                     # Simple GET endpoint
        "/delay/1",                                 # Endpoint with 1-second artificial delay
        "/status/200",                              # Endpoint that returns HTTP 200
        "/headers",                                 # Endpoint returning request headers
        "/ip",                                      # Endpoint returning client IP
    ],
    "virtual_users": 10,                            # Number of concurrent simulated users
    "ramp_up_seconds": 5,                           # Time (sec) to gradually start all users
    "test_duration_seconds": 30,                    # Total time (sec) to run the load test
    "think_time_seconds": 1,                        # Wait time (sec) between each request
    "timeout_seconds": 10,                          # Max wait (sec) for a single HTTP request
    "thresholds": {                                 # Performance pass/fail thresholds
        "max_avg_response_ms": 2000,               # Fail if avg response > 2000 ms
        "max_error_rate_pct": 5.0,                 # Fail if error rate > 5%
        "min_throughput_rps": 1.0,                 # Fail if throughput < 1 request/sec
    }
}

# =============================================================================
# STAGE 1: PLANNING AGENT
# Role: Understand the test goal and prepare the execution plan
# Algorithm: Parse config -> Validate inputs -> Compute schedule -> Output plan
# =============================================================================
class PlanningAgent:
    """
    Stage 1 - Planning Agent
    Responsible for validating configuration, computing test schedules,
    and producing a structured test plan used by all downstream agents.
    """

    def __init__(self, config: dict):
        self.config = config                        # Store the global configuration
        self.plan = {}                              # Output plan dictionary

    def validate_config(self) -> bool:
        """
        Validate that required config keys exist and have sensible values.
        Returns True if config is valid, raises ValueError otherwise.
        """
        required_keys = [                           # List of mandatory config keys
            "target_url", "endpoints", "virtual_users",
            "test_duration_seconds", "thresholds"
        ]
        for key in required_keys:                   # Check each required key exists
            if key not in self.config:
                raise ValueError(f"Missing config key: {key}")  # Raise if missing
        if self.config["virtual_users"] < 1:        # At least 1 virtual user required
            raise ValueError("virtual_users must be >= 1")
        if self.config["test_duration_seconds"] < 5:  # Minimum 5 seconds test duration
            raise ValueError("test_duration_seconds must be >= 5")
        return True                                 # Config is valid

    def compute_schedule(self) -> list:
        """
        Compute start delay for each virtual user during ramp-up phase.
        This spreads user starts evenly over the ramp_up_seconds window.
        Returns a list of (user_id, start_delay_seconds) tuples.
        """
        users = self.config["virtual_users"]        # Total number of virtual users
        ramp = self.config.get("ramp_up_seconds", 5)  # Ramp-up window in seconds
        schedule = []                               # List to hold user schedule entries
        for i in range(users):                      # Iterate over each virtual user
            delay = (i / users) * ramp             # Spread starts evenly across ramp
            schedule.append((i + 1, round(delay, 2)))  # Append (user_id, delay) pair
        return schedule                             # Return the computed schedule

    def run(self) -> dict:
        """
        Main entry point for the Planning Agent.
        Validates config, builds test plan, and returns it.
        """
        logger.info("=== STAGE 1: PLANNING AGENT STARTED ===")
        self.validate_config()                      # Validate config before proceeding
        schedule = self.compute_schedule()          # Compute user ramp-up schedule
        self.plan = {
            "target_url": self.config["target_url"],      # Base URL for the test
            "endpoints": self.config["endpoints"],         # Endpoints to be tested
            "virtual_users": self.config["virtual_users"], # Number of VUs
            "test_duration_seconds": self.config["test_duration_seconds"],
            "think_time_seconds": self.config.get("think_time_seconds", 1),
            "timeout_seconds": self.config.get("timeout_seconds", 10),
            "thresholds": self.config["thresholds"],      # Pass/fail thresholds
            "user_schedule": schedule,                     # Ramp-up timing per user
            "planned_at": datetime.utcnow().isoformat(),  # Timestamp of plan creation
        }
        logger.info(f"Test plan created: {self.config['virtual_users']} users, "
                    f"{self.config['test_duration_seconds']}s duration")
        logger.info("=== STAGE 1: PLANNING AGENT COMPLETED ===")
        return self.plan                            # Return the test plan


# =============================================================================
# STAGE 2: DISCOVERY AGENT
# Role: Probe the target application to verify connectivity and discover health
# Algorithm: Ping endpoints -> Measure baseline -> Detect availability
# =============================================================================
class DiscoveryAgent:
    """
    Stage 2 - Discovery Agent
    Probes each configured endpoint to verify the application is reachable,
    measure baseline response times, and detect any already-failing routes.
    """

    def __init__(self, plan: dict):
        self.plan = plan                            # Receive plan from Planning Agent
        self.discovery_results = []                 # List to store probe results

    def probe_endpoint(self, base_url: str, endpoint: str, timeout: int) -> dict:
        """
        Send a single HTTP GET request to the given endpoint.
        Returns a dict with status_code, response_time_ms, and error info.
        """
        url = base_url + endpoint                   # Construct full URL
        result = {
            "endpoint": endpoint,                   # Record which endpoint was probed
            "url": url,                             # Full URL that was requested
            "status_code": None,                    # Will hold HTTP status code
            "response_time_ms": None,               # Will hold response latency
            "reachable": False,                     # Default: not reachable
            "error": None                           # Will hold error message if any
        }
        try:
            req = Request(url, headers={"User-Agent": "PerfTestAgent/1.0"})
            start = time.time()                     # Record request start time
            with urlopen(req, timeout=timeout) as resp:  # Open HTTP connection
                result["status_code"] = resp.getcode()   # Capture HTTP status code
                result["response_time_ms"] = round((time.time() - start) * 1000, 2)
                result["reachable"] = True          # Mark as reachable on success
        except HTTPError as e:
            result["status_code"] = e.code          # Capture HTTP error code
            result["error"] = str(e)               # Record error message
        except URLError as e:
            result["error"] = str(e)               # Record connection error
        except Exception as e:
            result["error"] = str(e)               # Record any other exception
        return result                               # Return the probe result

    def run(self) -> list:
        """
        Probe all endpoints in the test plan and return discovery results.
        Logs a warning if any endpoint is unreachable.
        """
        logger.info("=== STAGE 2: DISCOVERY AGENT STARTED ===")
        base_url = self.plan["target_url"]          # Get base URL from plan
        timeout = self.plan["timeout_seconds"]       # Get timeout from plan
        for endpoint in self.plan["endpoints"]:      # Iterate over all endpoints
            result = self.probe_endpoint(base_url, endpoint, timeout)
            self.discovery_results.append(result)   # Collect each probe result
            status = result["status_code"] or "ERR" # Display status or ERR
            rt = result["response_time_ms"] or "N/A"
            logger.info(f"Probed {endpoint}: status={status}, rt={rt}ms")
            if not result["reachable"]:             # Warn if endpoint not reachable
                logger.warning(f"Endpoint {endpoint} is NOT reachable: {result['error']}")
        logger.info("=== STAGE 2: DISCOVERY AGENT COMPLETED ===")
        return self.discovery_results               # Return all probe results


# =============================================================================
# STAGE 3: WORKLOAD DESIGN AGENT
# Role: Design the load test scenarios and virtual user behaviors
# Algorithm: Analyze endpoints -> Build scenarios -> Assign weights
# =============================================================================
class WorkloadDesignAgent:
    """
    Stage 3 - Workload Design Agent
    Designs realistic user scenarios based on discovery results.
    Assigns probability weights so traffic mimics real usage patterns.
    """

    def __init__(self, plan: dict, discovery_results: list):
        self.plan = plan                            # Receive test plan
        self.discovery_results = discovery_results  # Receive probed endpoint data
        self.scenarios = []                         # List of designed test scenarios

    def assign_weights(self, reachable_endpoints: list) -> list:
        """
        Assign probability weights to each reachable endpoint.
        Endpoints with faster baseline response get slightly higher weight.
        Returns list of (endpoint, weight) tuples normalized to sum=1.0.
        """
        weights = []                                # List to build weight entries
        for ep_data in reachable_endpoints:         # Iterate over reachable endpoints
            rt = ep_data.get("response_time_ms") or 1000.0  # Default 1000ms if missing
            weight = 1.0 / (1.0 + rt / 1000.0)    # Faster endpoints get higher weight
            weights.append((ep_data["endpoint"], weight))   # Append (endpoint, weight)
        total = sum(w for _, w in weights)          # Sum all weights for normalization
        normalized = [(ep, round(w / total, 4)) for ep, w in weights]  # Normalize
        return normalized                           # Return normalized weight list

    def build_scenarios(self) -> list:
        """
        Build test scenarios from reachable discovery results.
        Each scenario defines which endpoint to call and how often (weight).
        """
        reachable = [r for r in self.discovery_results if r["reachable"]]
        if not reachable:                           # If no endpoints reachable
            raise RuntimeError("No reachable endpoints found by Discovery Agent")
        weighted = self.assign_weights(reachable)   # Get weighted endpoint list
        self.scenarios = []                         # Reset scenarios list
        for endpoint, weight in weighted:           # Build scenario per endpoint
            scenario = {
                "name": f"Test_{endpoint.strip('/').replace('/', '_') or 'root'}",
                "endpoint": endpoint,               # Endpoint path to request
                "method": "GET",                    # HTTP method (GET for these tests)
                "weight": weight,                   # Probability this scenario is chosen
                "headers": {                        # HTTP headers to send with request
                    "User-Agent": "PerfTestAgent/VirtualUser",
                    "Accept": "application/json"
                }
            }
            self.scenarios.append(scenario)         # Add scenario to list
        return self.scenarios                       # Return all designed scenarios

    def run(self) -> list:
        """
        Main entry point for Workload Design Agent.
        Builds and returns the list of test scenarios.
        """
        logger.info("=== STAGE 3: WORKLOAD DESIGN AGENT STARTED ===")
        scenarios = self.build_scenarios()          # Build all test scenarios
        for s in scenarios:                         # Log each scenario summary
            logger.info(f"Scenario: {s['name']}, weight={s['weight']}")
        logger.info(f"Total scenarios designed: {len(scenarios)}")
        logger.info("=== STAGE 3: WORKLOAD DESIGN AGENT COMPLETED ===")
        return scenarios                            # Return scenario list


# =============================================================================
# STAGE 4: EXECUTION AGENT
# Role: Run the load test by executing virtual users concurrently
# Algorithm: Spawn threads -> Ramp up -> Execute requests -> Collect results
# =============================================================================
class ExecutionAgent:
    """
    Stage 4 - Execution Agent
    Spawns multiple virtual user threads and runs the load test.
    Collects raw request/response metrics into a thread-safe queue.
    """

    def __init__(self, plan: dict, scenarios: list):
        self.plan = plan                            # Receive test plan
        self.scenarios = scenarios                  # Receive designed scenarios
        self.results_queue = queue.Queue()          # Thread-safe queue for results
        self.stop_event = threading.Event()         # Event to signal all VUs to stop

    def pick_scenario(self) -> dict:
        """
        Randomly select a scenario based on assigned probability weights.
        Uses a weighted random selection algorithm.
        Returns the selected scenario dict.
        """
        weights = [s["weight"] for s in self.scenarios]  # Extract weights list
        total = sum(weights)                         # Sum for normalization
        rand = random.uniform(0, total)             # Random value in [0, total]
        cumulative = 0.0                            # Running cumulative weight
        for i, w in enumerate(weights):             # Walk through scenarios
            cumulative += w                         # Add this scenario's weight
            if rand <= cumulative:                  # Check if random falls here
                return self.scenarios[i]            # Return selected scenario
        return self.scenarios[-1]                   # Fallback: return last scenario

    def virtual_user(self, user_id: int, start_delay: float):
        """
        Simulates a single virtual user making requests until stop_event is set.
        Each VU waits its start_delay, then loops: pick scenario, request, think.
        Results are pushed to the shared results_queue.
        """
        time.sleep(start_delay)                     # Wait for ramp-up delay
        base_url = self.plan["target_url"]          # Get base URL
        timeout = self.plan["timeout_seconds"]       # Get request timeout
        think_time = self.plan["think_time_seconds"] # Get think time between requests

        while not self.stop_event.is_set():         # Loop until test ends
            scenario = self.pick_scenario()          # Select a scenario randomly
            url = base_url + scenario["endpoint"]   # Construct full request URL
            result = {                              # Build result record
                "user_id": user_id,                # Which VU made this request
                "scenario": scenario["name"],       # Which scenario was executed
                "url": url,                        # Full URL requested
                "timestamp": time.time(),           # When request was sent
                "status_code": None,               # HTTP response status
                "response_time_ms": None,          # Latency in milliseconds
                "success": False,                  # Whether request succeeded
                "error": None                      # Error message if failed
            }
            try:
                req = Request(url, headers=scenario["headers"])  # Build HTTP request
                start = time.time()                # Start timer
                with urlopen(req, timeout=timeout) as resp:  # Execute request
                    result["status_code"] = resp.getcode()   # Capture status code
                    result["response_time_ms"] = round((time.time() - start) * 1000, 2)
                    result["success"] = True       # Mark as successful
            except HTTPError as e:
                result["status_code"] = e.code     # Capture HTTP error code
                result["response_time_ms"] = round((time.time() - start) * 1000, 2)
                result["error"] = f"HTTPError: {e.code}"
            except Exception as e:
                result["response_time_ms"] = round((time.time() - start) * 1000, 2)
                result["error"] = str(e)           # Capture exception message
            finally:
                self.results_queue.put(result)     # Push result to shared queue
            time.sleep(think_time)                 # Wait think time before next req

    def run(self) -> list:
        """
        Main entry point for Execution Agent.
        Starts all virtual user threads, waits for test duration, then stops them.
        Returns list of all collected raw results.
        """
        logger.info("=== STAGE 4: EXECUTION AGENT STARTED ===")
        duration = self.plan["test_duration_seconds"]  # Total test duration
        schedule = self.plan["user_schedule"]           # VU ramp-up schedule
        threads = []                                   # List of VU threads

        for user_id, delay in schedule:               # Start each virtual user thread
            t = threading.Thread(
                target=self.virtual_user,             # Thread function
                args=(user_id, delay),                # Pass user_id and start delay
                daemon=True                           # Daemon so test exits cleanly
            )
            threads.append(t)                         # Track thread
            t.start()                                 # Start the thread
            logger.info(f"VU {user_id} started with {delay}s ramp delay")

        logger.info(f"Load test running for {duration} seconds...")
        time.sleep(duration)                          # Wait for full test duration
        self.stop_event.set()                        # Signal all VUs to stop

        for t in threads:                             # Wait for all threads to finish
            t.join(timeout=5)                         # Join with 5s timeout per thread

        raw_results = []                              # Collect results from queue
        while not self.results_queue.empty():         # Drain the results queue
            raw_results.append(self.results_queue.get())

        logger.info(f"Total requests collected: {len(raw_results)}")
        logger.info("=== STAGE 4: EXECUTION AGENT COMPLETED ===")
        return raw_results                            # Return all raw request results


# =============================================================================
# STAGE 5: ANALYSIS AGENT
# Role: Process raw results and compute performance metrics
# Algorithm: Aggregate -> Compute stats -> Compare thresholds -> Score
# =============================================================================
class AnalysisAgent:
    """
    Stage 5 - Analysis Agent
    Takes raw execution results and computes key performance indicators (KPIs):
    - Average, Median, P95, P99 response times
    - Error rate percentage
    - Throughput (requests per second)
    - Pass/fail against configured thresholds
    """

    def __init__(self, plan: dict, raw_results: list):
        self.plan = plan                              # Receive test plan for thresholds
        self.raw_results = raw_results                # Receive raw request results
        self.metrics = {}                             # Dict to store computed metrics

    def compute_response_time_stats(self, times: list) -> dict:
        """
        Compute statistical summary of response times.
        Args: times - list of response time values in milliseconds
        Returns: dict with avg, median, p95, p99, min, max values
        """
        if not times:                                 # Guard against empty list
            return {"avg": 0, "median": 0, "p95": 0, "p99": 0, "min": 0, "max": 0}
        sorted_times = sorted(times)                  # Sort for percentile calculation
        n = len(sorted_times)                         # Total count of measurements
        p95_idx = int(n * 0.95)                       # Index for 95th percentile
        p99_idx = int(n * 0.99)                       # Index for 99th percentile
        return {
            "avg": round(statistics.mean(times), 2),  # Arithmetic mean
            "median": round(statistics.median(times), 2),  # 50th percentile
            "p95": sorted_times[min(p95_idx, n-1)],   # 95th percentile latency
            "p99": sorted_times[min(p99_idx, n-1)],   # 99th percentile latency
            "min": sorted_times[0],                    # Fastest request
            "max": sorted_times[-1],                   # Slowest request
        }

    def evaluate_thresholds(self, avg_ms: float, error_rate: float, rps: float) -> dict:
        """
        Compare computed metrics against configured thresholds.
        Returns a dict mapping each threshold check to PASS or FAIL.
        """
        thresholds = self.plan["thresholds"]          # Get threshold config
        return {
            "avg_response_time": (
                "PASS" if avg_ms <= thresholds["max_avg_response_ms"] else "FAIL"
            ),                                         # Check avg response time
            "error_rate": (
                "PASS" if error_rate <= thresholds["max_error_rate_pct"] else "FAIL"
            ),                                         # Check error rate percentage
            "throughput": (
                "PASS" if rps >= thresholds["min_throughput_rps"] else "FAIL"
            ),                                         # Check minimum throughput
        }

    def run(self) -> dict:
        """
        Main entry point for Analysis Agent.
        Computes all KPIs and threshold evaluations from raw results.
        Returns comprehensive metrics dict.
        """
        logger.info("=== STAGE 5: ANALYSIS AGENT STARTED ===")
        total = len(self.raw_results)                  # Total request count
        successful = [r for r in self.raw_results if r["success"]]   # Successful requests
        failed = [r for r in self.raw_results if not r["success"]]   # Failed requests

        # Compute error rate as percentage
        error_rate = round((len(failed) / total * 100) if total > 0 else 0, 2)

        # Extract all response times from successful results
        response_times = [r["response_time_ms"] for r in successful if r["response_time_ms"]]

        # Compute response time statistics
        rt_stats = self.compute_response_time_stats(response_times)

        # Compute throughput: requests per second over test duration
        duration = self.plan["test_duration_seconds"]
        rps = round(total / duration, 2) if duration > 0 else 0

        # Evaluate pass/fail against thresholds
        threshold_results = self.evaluate_thresholds(rt_stats["avg"], error_rate, rps)

        # Determine overall test result: PASS only if all thresholds pass
        overall = "PASS" if all(v == "PASS" for v in threshold_results.values()) else "FAIL"

        # Build comprehensive metrics dictionary
        self.metrics = {
            "total_requests": total,                   # Total HTTP requests made
            "successful_requests": len(successful),    # Requests with success=True
            "failed_requests": len(failed),            # Requests with success=False
            "error_rate_pct": error_rate,              # Error percentage
            "throughput_rps": rps,                     # Requests per second
            "response_time_ms": rt_stats,              # Full RT stats dict
            "threshold_results": threshold_results,    # Per-threshold PASS/FAIL
            "overall_result": overall,                 # PASS or FAIL overall
            "analyzed_at": datetime.utcnow().isoformat()  # Analysis timestamp
        }

        logger.info(f"Total: {total}, Success: {len(successful)}, Error Rate: {error_rate}%")
        logger.info(f"Avg RT: {rt_stats['avg']}ms, P95: {rt_stats['p95']}ms, RPS: {rps}")
        logger.info(f"Overall Result: {overall}")
        logger.info("=== STAGE 5: ANALYSIS AGENT COMPLETED ===")
        return self.metrics                            # Return computed metrics


# =============================================================================
# STAGE 6: REPORTING AGENT
# Role: Generate human-readable and machine-readable performance reports
# Algorithm: Format metrics -> Build text report -> Save JSON report
# =============================================================================
class ReportingAgent:
    """
    Stage 6 - Reporting Agent
    Formats the analysis metrics into a structured performance report,
    prints a human-readable summary to the console, and saves a JSON file.
    """

    def __init__(self, plan: dict, metrics: dict, discovery_results: list):
        self.plan = plan                              # Receive test plan
        self.metrics = metrics                        # Receive analysis metrics
        self.discovery_results = discovery_results    # Receive discovery probe data
        self.report = {}                              # Dict to hold full report

    def build_report(self) -> dict:
        """
        Build a complete structured report combining plan, discovery, and metrics.
        Returns the full report dictionary.
        """
        self.report = {
            "report_title": "Web Application Performance Test Report",
            "generated_at": datetime.utcnow().isoformat(),  # Report generation time
            "target_url": self.plan["target_url"],           # Tested application URL
            "test_configuration": {                          # Key test settings
                "virtual_users": self.plan["virtual_users"],
                "test_duration_seconds": self.plan["test_duration_seconds"],
                "think_time_seconds": self.plan["think_time_seconds"],
                "endpoints_tested": self.plan["endpoints"],
            },
            "discovery_summary": {                           # Endpoint availability info
                "probed_endpoints": len(self.discovery_results),
                "reachable": sum(1 for r in self.discovery_results if r["reachable"]),
                "unreachable": sum(1 for r in self.discovery_results if not r["reachable"]),
                "details": self.discovery_results            # Full probe details
            },
            "performance_metrics": self.metrics,             # All computed KPIs
            "recommendations": self.generate_recommendations()  # AI recommendations
        }
        return self.report                            # Return the full report

    def generate_recommendations(self) -> list:
        """
        Generate intelligent recommendations based on observed metrics.
        Uses simple rule-based AI logic to identify problem areas.
        Returns a list of recommendation strings.
        """
        recommendations = []                          # List of recommendation strings
        metrics = self.metrics                        # Short alias

        # Check average response time threshold
        if metrics.get("threshold_results", {}).get("avg_response_time") == "FAIL":
            recommendations.append(
                "HIGH LATENCY: Average response time exceeds threshold. "
                "Consider adding caching, CDN, or optimizing database queries."
            )

        # Check error rate threshold
        if metrics.get("threshold_results", {}).get("error_rate") == "FAIL":
            recommendations.append(
                "HIGH ERROR RATE: Too many requests are failing. "
                "Review server logs, check for 5xx errors, and increase server capacity."
            )

        # Check throughput threshold
        if metrics.get("threshold_results", {}).get("throughput") == "FAIL":
            recommendations.append(
                "LOW THROUGHPUT: Server cannot handle the required request rate. "
                "Consider horizontal scaling, load balancing, or async processing."
            )

        # Check P95 latency even if avg passes
        p95 = metrics.get("response_time_ms", {}).get("p95", 0)
        if p95 > 3000:                                # If P95 > 3 seconds
            recommendations.append(
                f"P95 LATENCY ({p95}ms) is very high. "
                "Investigate slow outlier requests and optimize worst-case code paths."
            )

        # If all thresholds passed with no issues
        if not recommendations:
            recommendations.append(
                "All performance thresholds PASSED. Application is performing well "
                "under the configured load. Continue monitoring in production."
            )

        return recommendations                        # Return generated recommendations

    def print_report(self):
        """
        Print a formatted human-readable summary of the test report to console.
        Uses separator lines for clear section boundaries.
        """
        m = self.metrics                              # Short alias for metrics
        rt = m.get("response_time_ms", {})            # Response time sub-dict
        thr = m.get("threshold_results", {})          # Threshold results sub-dict

        print("\n" + "="*70)
        print("  WEB APPLICATION PERFORMANCE TEST REPORT")
        print("="*70)
        print(f"  Target URL    : {self.plan['target_url']}")
        print(f"  Generated At  : {self.report.get('generated_at', 'N/A')}")
        print(f"  Virtual Users : {self.plan['virtual_users']}")
        print(f"  Duration      : {self.plan['test_duration_seconds']}s")
        print("-"*70)
        print("  DISCOVERY SUMMARY")
        print("-"*70)
        disc = self.report.get("discovery_summary", {})
        print(f"  Probed        : {disc.get('probed_endpoints', 0)} endpoints")
        print(f"  Reachable     : {disc.get('reachable', 0)}")
        print(f"  Unreachable   : {disc.get('unreachable', 0)}")
        print("-"*70)
        print("  PERFORMANCE METRICS")
        print("-"*70)
        print(f"  Total Requests: {m.get('total_requests', 0)}")
        print(f"  Successful    : {m.get('successful_requests', 0)}")
        print(f"  Failed        : {m.get('failed_requests', 0)}")
        print(f"  Error Rate    : {m.get('error_rate_pct', 0)}%")
        print(f"  Throughput    : {m.get('throughput_rps', 0)} req/sec")
        print(f"  Avg RT        : {rt.get('avg', 0)}ms")
        print(f"  Median RT     : {rt.get('median', 0)}ms")
        print(f"  P95 RT        : {rt.get('p95', 0)}ms")
        print(f"  P99 RT        : {rt.get('p99', 0)}ms")
        print(f"  Min RT        : {rt.get('min', 0)}ms")
        print(f"  Max RT        : {rt.get('max', 0)}ms")
        print("-"*70)
        print("  THRESHOLD EVALUATION")
        print("-"*70)
        for check, result in thr.items():             # Print each threshold result
            print(f"  {check:<25}: {result}")
        print(f"  {'OVERALL RESULT':<25}: {m.get('overall_result', 'N/A')}")
        print("-"*70)
        print("  AI RECOMMENDATIONS")
        print("-"*70)
        for i, rec in enumerate(self.report.get("recommendations", []), 1):
            print(f"  {i}. {rec}")                    # Print each recommendation
        print("="*70 + "\n")

    def save_report(self, filename: str = "perf_test_report.json"):
        """
        Save the full report as a JSON file for machine-readable archiving.
        Args: filename - output file path (default: perf_test_report.json)
        """
        with open(filename, "w") as f:                # Open file for writing
            json.dump(self.report, f, indent=2)       # Write pretty-printed JSON
        logger.info(f"Report saved to: {filename}")   # Log the file path

    def run(self):
        """
        Main entry point for Reporting Agent.
        Builds the report, prints it, and saves the JSON file.
        """
        logger.info("=== STAGE 6: REPORTING AGENT STARTED ===")
        self.build_report()                           # Build the full report structure
        self.print_report()                           # Print human-readable summary
        self.save_report("perf_test_report.json")     # Save machine-readable JSON
        logger.info("=== STAGE 6: REPORTING AGENT COMPLETED ===")
        return self.report                            # Return the full report dict


# =============================================================================
# MAIN ORCHESTRATOR
# Chains all 6 intelligent agents in sequence, passing outputs as inputs
# =============================================================================
def run_performance_test(config: dict = None):
    """
    Main orchestrator function that chains all 6 AI agents in order:
    Stage 1 (Plan) -> Stage 2 (Discover) -> Stage 3 (Design) ->
    Stage 4 (Execute) -> Stage 5 (Analyze) -> Stage 6 (Report)

    Args: config - optional override config dict (uses global CONFIG if None)
    Returns: The final performance test report dictionary
    """
    cfg = config or CONFIG                            # Use provided config or default

    # ---- STAGE 1: Planning Agent ----
    planner = PlanningAgent(cfg)                      # Instantiate Planning Agent
    plan = planner.run()                              # Generate test plan

    # ---- STAGE 2: Discovery Agent ----
    discoverer = DiscoveryAgent(plan)                 # Instantiate Discovery Agent
    discovery_results = discoverer.run()              # Probe all endpoints

    # ---- STAGE 3: Workload Design Agent ----
    designer = WorkloadDesignAgent(plan, discovery_results)  # Instantiate Designer
    scenarios = designer.run()                        # Build test scenarios

    # ---- STAGE 4: Execution Agent ----
    executor = ExecutionAgent(plan, scenarios)         # Instantiate Execution Agent
    raw_results = executor.run()                      # Run load test, collect results

    # ---- STAGE 5: Analysis Agent ----
    analyzer = AnalysisAgent(plan, raw_results)       # Instantiate Analysis Agent
    metrics = analyzer.run()                          # Compute all KPIs

    # ---- STAGE 6: Reporting Agent ----
    reporter = ReportingAgent(plan, metrics, discovery_results)  # Instantiate Reporter
    report = reporter.run()                           # Generate and save report

    return report                                     # Return the final report


# =============================================================================
# ENTRY POINT
# Run the full 6-stage performance test when script is executed directly
# =============================================================================
if __name__ == "__main__":
    # Run performance test with the global CONFIG settings
    # Modify CONFIG at the top of this file to target your application
    final_report = run_performance_test()
    print(f"Test completed. Overall result: {final_report['performance_metrics']['overall_result']}")
