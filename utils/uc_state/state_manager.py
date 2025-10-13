"""
UC-State: Unity Catalog-based state management utility
Manages Databricks resource state using Unity Catalog tables.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo
import logging

logger = logging.getLogger(__name__)


class UCState:
    """Unity Catalog-based state management for Databricks resources."""
    
    def __init__(self, catalog: str, schema: str = "_internal_state", table: str = "resources"):
        """
        Initialize UC-State manager.
        
        Args:
            catalog: The catalog name to store state in
            schema: Schema name (default: _internal_state)  
            table: Table name (default: resources)
        """
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.full_table_name = f"{catalog}.{schema}.{table}"
        self.w = WorkspaceClient()
        
        self._ensure_catalog_schema_table()
    
    def _ensure_catalog_schema_table(self):
        """Ensure the catalog, schema and table exist."""
        try:
            # Check if catalog exists
            try:
                self.w.catalogs.get(self.catalog)
            except Exception:
                logger.warning(f"Catalog {self.catalog} may not exist or not accessible")
                
            # Create schema if it doesn't exist
            try:
                self.w.schemas.get(f"{self.catalog}.{self.schema}")
            except Exception:
                try:
                    self.w.schemas.create(
                        name=self.schema,
                        catalog_name=self.catalog,
                        comment="UC-State schema for resource management"
                    )
                    logger.info(f"Created schema {self.catalog}.{self.schema}")
                except Exception as e:
                    logger.warning(f"Could not create schema: {e}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Error ensuring catalog/schema/table: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the state table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            internal_id STRING NOT NULL,
            resource_type STRING NOT NULL,
            resource_data STRING NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (internal_id)
        ) USING DELTA
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
        """
        
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                spark.sql(create_table_sql)
                logger.info(f"Ensured table {self.full_table_name} exists")
            else:
                logger.warning("No active Spark session found, cannot create table")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def add(self, resource_type: str, resource_obj: Any) -> str:
        """
        Add a resource to state.

        Args:
            resource_type: Type of resource (experiments, jobs, pipelines, apps, databaseinstances, endpoints, catalogs, databasecatalogs, warehouses)
            resource_obj: The API return object from Databricks SDK or a dict with resource metadata
            
        Returns:
            internal_id: Generated UUID for this resource
        """
        internal_id = str(uuid.uuid4())
        
        # Convert resource object to JSON string
        if hasattr(resource_obj, 'as_dict'):
            resource_data = json.dumps(resource_obj.as_dict())
        elif hasattr(resource_obj, '__dict__'):
            # Handle objects without as_dict() method
            resource_data = json.dumps(resource_obj.__dict__, default=str)
        else:
            # Handle primitive types or dictionaries
            resource_data = json.dumps(resource_obj, default=str)
        
        insert_sql = f"""
        INSERT INTO {self.full_table_name} 
        (internal_id, resource_type, resource_data, created_at)
        VALUES ('{internal_id}', '{resource_type}', '{resource_data}', CURRENT_TIMESTAMP())
        """
        
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                spark.sql(insert_sql)
                logger.info(f"Added {resource_type} resource with ID {internal_id}")
                return internal_id
            else:
                raise RuntimeError("No active Spark session found")
        except Exception as e:
            logger.error(f"Error adding resource: {e}")
            raise
    
    def list(self, resource_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List resources in state.
        
        Args:
            resource_type: Filter by resource type (optional)
            
        Returns:
            List of state records
        """
        where_clause = f"WHERE resource_type = '{resource_type}'" if resource_type else ""
        query_sql = f"""
        SELECT internal_id, resource_type, resource_data, created_at 
        FROM {self.full_table_name} 
        {where_clause}
        ORDER BY created_at DESC
        """
        
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                df = spark.sql(query_sql)
                results = []
                for row in df.collect():
                    results.append({
                        'internal_id': row['internal_id'],
                        'resource_type': row['resource_type'],
                        'resource_data': json.loads(row['resource_data']),
                        'created_at': row['created_at']
                    })
                return results
            else:
                raise RuntimeError("No active Spark session found")
        except Exception as e:
            logger.error(f"Error listing resources: {e}")
            raise
    
    def remove(self, internal_id: str) -> bool:
        """
        Remove a resource from state by internal ID.
        
        Args:
            internal_id: The internal UUID of the resource
            
        Returns:
            True if removed, False if not found
        """
        delete_sql = f"""
        DELETE FROM {self.full_table_name} 
        WHERE internal_id = '{internal_id}'
        """
        
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                # Check if exists first
                exists_df = spark.sql(f"SELECT 1 FROM {self.full_table_name} WHERE internal_id = '{internal_id}'")
                if exists_df.count() == 0:
                    logger.warning(f"Resource with ID {internal_id} not found")
                    return False
                
                spark.sql(delete_sql)
                logger.info(f"Removed resource with ID {internal_id}")
                return True
            else:
                raise RuntimeError("No active Spark session found")
        except Exception as e:
            logger.error(f"Error removing resource: {e}")
            raise
    
    def clear_all(self, dry_run: bool = False) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
        """
        Remove all resources from Databricks and clear state.
        Deletion order: experiments → jobs → pipelines → endpoints → apps → warehouses → databaseinstances → catalogs
        
        Args:
            dry_run: If True, only show what would be deleted without actually deleting
            
        Returns:
            Dict with structure: {
                "resource_type": {
                    "successful": [{"id": "internal_id", "name": "resource_name"}],
                    "failed": [{"id": "internal_id", "name": "resource_name", "reason": "error_message"}]
                }
            }
        """
        # Define deletion order: experiments → jobs → pipelines → endpoints → apps → warehouses → databasecatalogs → catalogs → databaseinstances
        deletion_order = ['experiments', 'jobs', 'pipelines', 'endpoints', 'apps', 'warehouses', 'databasecatalogs', 'catalogs', 'databaseinstances']
        results = {}
        
        for resource_type in deletion_order:
            results[resource_type] = {"successful": [], "failed": []}
            
            try:
                resources = self.list(resource_type)
            except Exception as e:
                logger.error(f"Error listing {resource_type}: {e}")
                results[resource_type]["failed"].append({
                    "id": "N/A", 
                    "name": "N/A", 
                    "reason": f"Failed to list {resource_type}: {str(e)}"
                })
                continue
            
            for resource in resources:
                resource_data = resource['resource_data']
                internal_id = resource['internal_id']
                
                # Extract resource name for better reporting
                resource_name = "Unknown"
                if resource_type == 'experiments':
                    resource_name = resource_data.get('name', 'Unknown')
                elif resource_type == 'jobs':
                    resource_name = resource_data.get('settings', {}).get('name') or resource_data.get('job_id', 'Unknown')
                elif resource_type == 'pipelines':
                    resource_name = resource_data.get('name') or resource_data.get('pipeline_id', 'Unknown')
                elif resource_type == 'endpoints':
                    resource_name = resource_data.get('endpoint_name', 'Unknown')
                elif resource_type == 'apps':
                    resource_name = resource_data.get('name', 'Unknown')
                elif resource_type == 'warehouses':
                    resource_name = resource_data.get('name') or resource_data.get('id', 'Unknown')
                elif resource_type == 'databaseinstances':
                    resource_name = resource_data.get('name', 'Unknown')
                elif resource_type == 'databasecatalogs':
                    resource_name = resource_data if isinstance(resource_data, str) else resource_data.get('name', 'Unknown')
                elif resource_type == 'catalogs':
                    resource_name = resource_data if isinstance(resource_data, str) else resource_data.get('name', 'Unknown')
                
                if dry_run:
                    results[resource_type]["successful"].append({
                        "id": internal_id, 
                        "name": resource_name
                    })
                    continue
                
                # Attempt to delete the resource from Databricks
                deletion_successful = False
                error_message = None

                try:
                    if resource_type == 'experiments':
                        experiment_id = resource_data.get('experiment_id')
                        if experiment_id:
                            import mlflow
                            client = mlflow.MlflowClient()
                            client.delete_experiment(experiment_id)
                            logger.info(f"Deleted experiment {experiment_id}")
                            deletion_successful = True
                        else:
                            error_message = "No experiment_id found in resource data"

                    elif resource_type == 'jobs':
                        job_id = resource_data.get('job_id')
                        if job_id:
                            self.w.jobs.delete(job_id=int(job_id))
                            logger.info(f"Deleted job {job_id}")
                            deletion_successful = True
                        else:
                            error_message = "No job_id found in resource data"
                    
                    elif resource_type == 'pipelines':
                        pipeline_id = resource_data.get('pipeline_id')
                        if pipeline_id:
                            self.w.pipelines.delete(pipeline_id=pipeline_id)
                            logger.info(f"Deleted pipeline {pipeline_id}")
                            deletion_successful = True
                        else:
                            error_message = "No pipeline_id found in resource data"
                    
                    elif resource_type == 'endpoints':
                        endpoint_name = resource_data.get('endpoint_name')
                        if endpoint_name:
                            from mlflow.deployments import get_deploy_client
                            client = get_deploy_client("databricks")
                            client.delete_endpoint(endpoint=endpoint_name)
                            logger.info(f"Deleted serving endpoint {endpoint_name}")
                            deletion_successful = True
                        else:
                            error_message = "No endpoint name found in resource data"
                    
                    elif resource_type == 'apps':
                        app_name = resource_data.get('name')
                        if app_name:
                            self.w.apps.delete(app_name)
                            logger.info(f"Deleted app {app_name}")
                            deletion_successful = True
                        else:
                            error_message = "No app name found in resource data"
                    
                    elif resource_type == 'warehouses':
                        warehouse_id = resource_data.get('id')
                        if warehouse_id:
                            self.w.warehouses.delete(id=warehouse_id)
                            logger.info(f"Deleted warehouse {warehouse_id}")
                            deletion_successful = True
                        else:
                            error_message = "No warehouse id found in resource data"
                    
                    elif resource_type == 'databaseinstances':
                        instance_name = resource_data.get('name')
                        if instance_name:
                            self.w.database.delete_database_instance(name=instance_name)
                            logger.info(f"Deleted database instance {instance_name}")
                            deletion_successful = True
                        else:
                            error_message = "No instance name found in resource data"
                    
                    elif resource_type == 'databasecatalogs':
                        catalog_name = resource_data if isinstance(resource_data, str) else resource_data.get('name')
                        if catalog_name:
                            self.w.database.delete_database_catalog(name=catalog_name)
                            logger.info(f"Deleted database catalog {catalog_name}")
                            deletion_successful = True
                        else:
                            error_message = "No database catalog name found in resource data"
                    
                    elif resource_type == 'catalogs':
                        catalog_name = resource_data if isinstance(resource_data, str) else resource_data.get('name')
                        if catalog_name:
                            self.w.catalogs.delete(name=catalog_name, force=True)
                            logger.info(f"Deleted catalog {catalog_name}")
                            deletion_successful = True
                        else:
                            error_message = "No catalog name found in resource data"
                    
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Error deleting {resource_type} {resource_name} (ID: {internal_id}): {e}")
                
                # Remove from state if deletion was successful
                if deletion_successful:
                    try:
                        self.remove(internal_id)
                        results[resource_type]["successful"].append({
                            "id": internal_id, 
                            "name": resource_name
                        })
                    except Exception as e:
                        logger.error(f"Error removing {internal_id} from state: {e}")
                        results[resource_type]["failed"].append({
                            "id": internal_id, 
                            "name": resource_name, 
                            "reason": f"Resource deleted but failed to remove from state: {str(e)}"
                        })
                else:
                    results[resource_type]["failed"].append({
                        "id": internal_id, 
                        "name": resource_name, 
                        "reason": error_message or "Unknown deletion error"
                    })
        
        # Clear any remaining state entries if not in dry run mode
        if not dry_run:
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                if spark:
                    spark.sql(f"DELETE FROM {self.full_table_name}")
                    logger.info("Cleared all remaining state entries")
            except Exception as e:
                logger.error(f"Error clearing remaining state: {e}")
                # Add this to results as a special case
                if "cleanup" not in results:
                    results["cleanup"] = {"successful": [], "failed": []}
                results["cleanup"]["failed"].append({
                    "id": "state_table", 
                    "name": self.full_table_name, 
                    "reason": f"Failed to clear state table: {str(e)}"
                })
        
        return results
    
    def get_resource_by_id(self, internal_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific resource by internal ID.
        
        Args:
            internal_id: The internal UUID of the resource
            
        Returns:
            Resource record or None if not found
        """
        query_sql = f"""
        SELECT internal_id, resource_type, resource_data, created_at 
        FROM {self.full_table_name} 
        WHERE internal_id = '{internal_id}'
        """
        
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                df = spark.sql(query_sql)
                rows = df.collect()
                if rows:
                    row = rows[0]
                    return {
                        'internal_id': row['internal_id'],
                        'resource_type': row['resource_type'],
                        'resource_data': json.loads(row['resource_data']),
                        'created_at': row['created_at']
                    }
                return None
            else:
                raise RuntimeError("No active Spark session found")
        except Exception as e:
            logger.error(f"Error getting resource by ID: {e}")
            raise
    
# Convenience function for adding resources without creating state manager explicitly
def add(catalog: str, resource_type: str, resource_obj: Any, schema: str = "_internal_state", table: str = "resources") -> str:
    """
    Add a resource to state without creating state manager explicitly.

    Args:
        catalog: The catalog name to store state in
        resource_type: Type of resource (experiments, jobs, pipelines, apps, databaseinstances, endpoints, catalogs, databasecatalogs, warehouses)
        resource_obj: The API return object from Databricks SDK or a dict with resource metadata
        schema: Schema name (default: _internal_state)
        table: Table name (default: resources)
        
    Returns:
        internal_id: Generated UUID for this resource
    """
    state_manager = UCState(catalog=catalog, schema=schema, table=table)
    return state_manager.add(resource_type, resource_obj)


# Convenience function for easy initialization
def create_state_manager(catalog: str, schema: str = "_internal_state", table: str = "resources") -> UCState:
    """
    Create a UC-State manager instance.
    
    Args:
        catalog: The catalog name to store state in
        schema: Schema name (default: _caspers_state)
        table: Table name (default: resources)
        
    Returns:
        UCState instance
    """
    return UCState(catalog=catalog, schema=schema, table=table)