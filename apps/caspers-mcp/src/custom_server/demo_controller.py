"""Simple demo controller for Casper's Ghost Kitchen."""

import logging
from typing import Dict, Optional
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

class DemoController:
    """Simple controller for Casper's demo."""
    
    def __init__(self):
        self.w = WorkspaceClient()
        self.active_runs = {}  # catalog -> run_id mapping
        
    def start_demo(self, catalog: str) -> Dict:
        """Start demo by running init.ipynb then the created job."""
        try:
            current_user = self.w.current_user.me()
            init_path = f"/Users/{current_user.user_name}/databricks/init"
            
            # First run init.ipynb to create the job
            init_run = self.w.jobs.submit(
                run_name=f"Init Caspers Job - {catalog}",
                tasks=[{
                    "task_key": "init",
                    "notebook_task": {
                        "notebook_path": init_path,
                        "source": "WORKSPACE",
                        "base_parameters": {"CATALOG": catalog}
                    },
                }]
            )
            
            # Wait for init to complete
            init_result = self.w.jobs.wait_get_run_job_terminated_or_skipped(init_run.run_id)
            if init_result.state.result_state.value != "SUCCESS":
                return {"status": "error", "message": "Failed to create Casper's Initializer job"}
            
            # Find the newly created job
            jobs = self.w.jobs.list()
            job_id = None
            for job in jobs:
                if job.settings and job.settings.name == "Casper's Initializer":
                    job_id = job.job_id
                    break
            
            if not job_id:
                return {"status": "error", "message": "Casper's Initializer job was not created"}
            
            # Now run the actual demo job
            demo_run = self.w.jobs.run_now(job_id=job_id, job_parameters={"CATALOG": catalog})
            self.active_runs[catalog] = demo_run.run_id
            
            return {
                "status": "started",
                "catalog": catalog,
                "init_run_id": init_run.run_id,
                "demo_run_id": demo_run.run_id,
                "message": f"Demo started for catalog: {catalog}"
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def get_demo_status(self, catalog: str = None) -> Dict:
        """Get demo status."""
        try:
            if catalog and catalog in self.active_runs:
                run = self.w.jobs.get_run(self.active_runs[catalog])
                return {
                    "catalog": catalog,
                    "run_id": self.active_runs[catalog],
                    "state": run.state.life_cycle_state.value,
                    "result": run.state.result_state.value if run.state.result_state else None,
                    "url": run.run_page_url
                }
            
            # Return all active runs
            statuses = {}
            for cat, run_id in self.active_runs.items():
                try:
                    run = self.w.jobs.get_run(run_id)
                    statuses[cat] = {
                        "run_id": run_id,
                        "state": run.state.life_cycle_state.value,
                        "result": run.state.result_state.value if run.state.result_state else None
                    }
                except:
                    statuses[cat] = {"run_id": run_id, "state": "UNKNOWN"}
            
            return {"active_demos": statuses}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def cleanup_demo(self, catalog: str) -> Dict:
        """Run cleanup for specified catalog."""
        try:
            current_user = self.w.current_user.me()
            destroy_path = f"/Users/{current_user.user_name}/databricks/destroy"
            
            run_response = self.w.jobs.submit(
                run_name=f"Cleanup-{catalog}",
                tasks=[{
                    "task_key": "cleanup",
                    "notebook_task": {
                        "notebook_path": destroy_path,
                        "source": "WORKSPACE",
                        "base_parameters": {"CATALOG": catalog}
                    },
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 1
                    }
                }]
            )
            
            return {
                "status": "cleanup_started",
                "catalog": catalog,
                "cleanup_run_id": run_response.run_id
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}