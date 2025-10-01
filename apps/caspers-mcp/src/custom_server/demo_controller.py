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
            from databricks.sdk.service.jobs import JobSettings as Job
            import os

            git_folder_path = os.getenv("GIT_FOLDER_PATH", "")

            Casper_s_Initializer = Job.from_dict(
                {
                    "name": "Casper's Initializer",
                    "tasks": [
                        {
                            "task_key": "Raw_Data",
                            "notebook_task": {
                                "notebook_path": f"{git_folder_path}/stages/raw_data",
                                "source": "WORKSPACE",
                            },
                        },
                        {
                            "task_key": "Lakeflow_Declarative_Pipeline",
                            "depends_on": [
                                {
                                    "task_key": "Raw_Data",
                                },
                            ],
                            "notebook_task": {
                                "notebook_path": f"{git_folder_path}/stages/lakeflow",
                                "source": "WORKSPACE",
                            },
                        },
                        {
                            "task_key": "Refund_Recommender_Agent",
                            "depends_on": [
                                {
                                    "task_key": "Lakeflow_Declarative_Pipeline",
                                },
                            ],
                            "notebook_task": {
                                "notebook_path": f"{git_folder_path}/stages/refunder_agent",
                                "source": "WORKSPACE",
                            },
                        },
                        {
                            "task_key": "Refund_Recommender_Stream",
                            "depends_on": [
                                {
                                    "task_key": "Lakeflow_Declarative_Pipeline",
                                },
                            ],
                            "notebook_task": {
                                "notebook_path": f"{git_folder_path}/stages/refunder_stream",
                                "source": "WORKSPACE",
                            },
                        },
                        {
                            "task_key": "Lakebase_Reverse_ETL",
                            "depends_on": [
                                {
                                    "task_key": "Refund_Recommender_Stream",
                                },
                            ],
                            "notebook_task": {
                                "notebook_path": f"{git_folder_path}/stages/lakebase",
                                "source": "WORKSPACE",
                            },
                        },
                        {
                            "task_key": "Databricks_App_Refund_Manager",
                            "depends_on": [
                                {
                                    "task_key": "Lakebase_Reverse_ETL",
                                },
                            ],
                            "notebook_task": {
                                "notebook_path": f"{git_folder_path}/stages/apps",
                                "source": "WORKSPACE",
                            },
                        },
                    ],
                    "queue": {
                        "enabled": True,
                    },
                    "parameters": [
                        {
                            "name": "CATALOG",
                            "default": f"{catalog}",
                        },
                        {
                            "name": "EVENTS_VOLUME",
                            "default": "events",
                        },
                        {
                            "name": "LLM_MODEL",
                            "default": "databricks-meta-llama-3-3-70b-instruct",
                        },
                        {
                            "name": "REFUND_AGENT_ENDPOINT_NAME",
                            "default": "caspers_refund_agent",
                        },
                        {
                            "name": "SIMULATOR_SCHEMA",
                            "default": "simulator",
                        },
                    ],
                    "performance_target": "PERFORMANCE_OPTIMIZED",
                }
            )
            jobresponse = self.w.jobs.create(**Casper_s_Initializer.as_shallow_dict())
            run = self.w.jobs.run_now(job_id=jobresponse.job_id)

            return {
                "status": "started",
                "catalog": catalog,
                "init_run_id": run.run_id,
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