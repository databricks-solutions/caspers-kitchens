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
        
    def _find_caspers_job(self) -> Optional[int]:
        """Find Casper's Initializer job."""
        jobs = self.w.jobs.list()
        for job in jobs:
            if job.settings and job.settings.name == "Casper's Initializer":
                return job.job_id
        return None
    
    def start_demo(self, catalog: str) -> Dict:
        """Start demo with specified catalog."""
        try:
            job_id = self._find_caspers_job()
            if not job_id:
                return {"status": "error", "message": "Casper's Initializer job not found"}
            
            run_response = self.w.jobs.run_now(job_id=job_id, job_parameters={"CATALOG": catalog})
            self.active_runs[catalog] = run_response.run_id
            
            return {
                "status": "started",
                "catalog": catalog,
                "run_id": run_response.run_id,
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