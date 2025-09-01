"""
GitHub Organization Data Collection DAG
Collects repository data including stars, forks, and contributions
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dags.utils.github_client import GitHubDataCollector
from dags.utils.data_processor import DataProcessor
import json
import os
from typing import Dict, List

# Default arguments
default_args = {
  'owner': 'data-engineering',
  'depends_on_past': False,
  'start_date': datetime(2024, 1, 1),
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=5),
  'email': ['admin@company.com']
}

# DAG definition
dag = DAG(
  'github_org_data_collection',
  default_args=default_args,
  description='Collect GitHub organization repository data',
  schedule='@daily',  # Run daily
  catchup=False,
  max_active_runs=1,
  tags=['github', 'data-collection', 'api']
)


def collect_org_repositories(**context):
  """Collect all repositories from specified GitHub organizations"""
  # GitHub organizations to monitor (can be configured via Airflow Variables)
  from airflow.models import Variable

  try:
    orgs = Variable.get("github_organizations", deserialize_json=True)
  except:
    # Default organizations if variable not set
    orgs = ["apache", "kubernetes", "tensorflow", "microsoft"]

  github_token = Variable.get("github_token")
  collector = GitHubDataCollector(github_token)

  all_repo_data = []

  for org in orgs:
    print(f"Collecting data for organization: {org}")
    try:
      repos = collector.get_organization_repos(org)
      for repo in repos:
        repo_data = {
          'organization': org,
          'name': repo.name,
          'full_name': repo.full_name,
          'description': repo.description,
          'stars': repo.stargazers_count,
          'forks': repo.forks_count,
          'watchers': repo.watchers_count,
          'open_issues': repo.open_issues_count,
          'language': repo.language,
          'size': repo.size,
          'created_at': repo.created_at.isoformat() if repo.created_at else None,
          'updated_at': repo.updated_at.isoformat() if repo.updated_at else None,
          'pushed_at': repo.pushed_at.isoformat() if repo.pushed_at else None,
          'clone_url': repo.clone_url,
          'html_url': repo.html_url,
          'default_branch': repo.default_branch,
          'archived': repo.archived,
          'disabled': repo.disabled,
          'private': repo.private,
          'has_wiki': repo.has_wiki,
          'has_pages': repo.has_pages,
          'has_issues': repo.has_issues,
          'collection_date': context['ds']
        }
        all_repo_data.append(repo_data)

    except Exception as e:
      print(f"Error collecting data for org {org}: {str(e)}")
      continue

  # Save raw data
  output_file = f"/opt/airflow/output/repos_raw_{context['ds']}.json"
  os.makedirs(os.path.dirname(output_file), exist_ok=True)

  with open(output_file, 'w') as f:
    json.dump(all_repo_data, f, indent=2, default=str)

  print(f"Collected data for {len(all_repo_data)} repositories")
  return output_file


def collect_contribution_data(**context):
  """Collect contribution data for top repositories"""
  from airflow.models import Variable

  github_token = Variable.get("github_token")
  collector = GitHubDataCollector(github_token)

  # Read the previously collected repo data
  repo_file = context['task_instance'].xcom_pull(task_ids='collect_repositories')

  with open(repo_file, 'r') as f:
    repos_data = json.load(f)

  # Sort by stars and take top 50 repos to avoid API rate limits
  top_repos = sorted(repos_data, key=lambda x: x['stars'], reverse=True)[:50]

  contribution_data = []

  for repo in top_repos:
    try:
      print(f"Collecting contributions for {repo['full_name']}")
      contributors = collector.get_repository_contributors(repo['full_name'])

      repo_contributions = {
        'repo_full_name': repo['full_name'],
        'organization': repo['organization'],
        'repo_stars': repo['stars'],
        'contributors': [],
        'total_contributors': len(contributors),
        'collection_date': context['ds']
      }

      for contributor in contributors[:20]:  # Top 20 contributors
        contrib_data = {
          'login': contributor.login,
          'contributions': contributor.contributions,
          'avatar_url': contributor.avatar_url,
          'html_url': contributor.html_url,
          'type': contributor.type
        }
        repo_contributions['contributors'].append(contrib_data)

      contribution_data.append(repo_contributions)

    except Exception as e:
      print(f"Error collecting contributions for {repo['full_name']}: {str(e)}")
      continue

  # Save contribution data
  output_file = f"/opt/airflow/output/contributions_{context['ds']}.json"

  with open(output_file, 'w') as f:
    json.dump(contribution_data, f, indent=2, default=str)

  print(f"Collected contribution data for {len(contribution_data)} repositories")
  return output_file


def process_and_aggregate_data(**context):
  """Process and aggregate the collected data"""
  processor = DataProcessor()

  # Get file paths from previous tasks
  repo_file = context['task_instance'].xcom_pull(task_ids='collect_repositories')
  contrib_file = context['task_instance'].xcom_pull(task_ids='collect_contributions')

  # Process the data
  summary = processor.create_summary_report(repo_file, contrib_file)

  # Save summary report
  summary_file = f"/opt/airflow/output/github_summary_{context['ds']}.json"

  with open(summary_file, 'w') as f:
    json.dump(summary, f, indent=2, default=str)

  print("Data processing and aggregation completed")
  return summary_file


def cleanup_old_files(**context):
  """Clean up files older than 7 days"""
  import glob
  from pathlib import Path

  output_dir = Path("/opt/airflow/output")
  current_date = datetime.strptime(context['ds'], '%Y-%m-%d')

  for file_path in glob.glob(str(output_dir / "*.json")):
    file_path = Path(file_path)
    try:
      # Extract date from filename
      date_str = file_path.name.split('_')[-1].replace('.json', '')
      file_date = datetime.strptime(date_str, '%Y-%m-%d')

      if (current_date - file_date).days > 7:
        file_path.unlink()
        print(f"Deleted old file: {file_path}")
    except (ValueError, IndexError):
      continue


# Define tasks
collect_repos_task = PythonOperator(
  task_id='collect_repositories',
  python_callable=collect_org_repositories,
  dag=dag
)

collect_contrib_task = PythonOperator(
  task_id='collect_contributions',
  python_callable=collect_contribution_data,
  dag=dag
)

process_data_task = PythonOperator(
  task_id='process_data',
  python_callable=process_and_aggregate_data,
  dag=dag
)

cleanup_task = PythonOperator(
  task_id='cleanup_old_files',
  python_callable=cleanup_old_files,
  dag=dag
)

validate_output = BashOperator(
  task_id='validate_output',
  bash_command="""
    echo "Validating output files..."
    ls -la /opt/airflow/output/
    echo "Files created successfully for {{ ds }}"
    """,
  dag=dag
)

# Set task dependencies
collect_repos_task >> collect_contrib_task >> process_data_task >> validate_output >> cleanup_task
