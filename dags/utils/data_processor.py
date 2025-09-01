"""
Data processing utilities for GitHub data
"""
import json
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import logging


class DataProcessor:
  """Process and aggregate GitHub data"""

  def __init__(self):
    self.logger = logging.getLogger(__name__)

  def create_summary_report(self, repo_file: str, contrib_file: str) -> Dict[str, Any]:
    """
    Create a summary report from collected data

    Args:
        repo_file: Path to repository data JSON file
        contrib_file: Path to contribution data JSON file

    Returns:
        Dictionary containing summary statistics
    """
    with open(repo_file, 'r') as f:
      repos_data = json.load(f)

    with open(contrib_file, 'r') as f:
      contrib_data = json.load(f)

    # Repository statistics
    repo_df = pd.DataFrame(repos_data)

    # Organization statistics
    org_stats = repo_df.groupby('organization').agg({
      'name': 'count',
      'stars': ['sum', 'mean', 'max'],
      'forks': ['sum', 'mean', 'max'],
      'size': 'sum',
      'open_issues': 'sum'
    }).round(2)

    # Language statistics
    language_counts = repo_df['language'].value_counts().head(10).to_dict()

    # Top repositories by stars
    top_starred = repo_df.nlargest(20, 'stars')[['full_name', 'stars', 'forks', 'language']].to_dict('records')

    # Top repositories by forks
    top_forked = repo_df.nlargest(20, 'forks')[['full_name', 'stars', 'forks', 'language']].to_dict('records')

    # Contribution statistics
    total_contributors = sum(item['total_contributors'] for item in contrib_data)
    avg_contributors = total_contributors / len(contrib_data) if contrib_data else 0

    # Most active contributors across all repos
    contributor_activity = {}
    for repo in contrib_data:
      for contributor in repo['contributors']:
        login = contributor['login']
        if login not in contributor_activity:
          contributor_activity[login] = {
            'total_contributions': 0,
            'repos_contributed': 0,
            'avatar_url': contributor['avatar_url'],
            'html_url': contributor['html_url']
          }
        contributor_activity[login]['total_contributions'] += contributor['contributions']
        contributor_activity[login]['repos_contributed'] += 1

    # Sort by total contributions
    top_contributors = sorted(
      contributor_activity.items(),
      key=lambda x: x[1]['total_contributions'],
      reverse=True
    )[:20]

    summary = {
      'collection_date': datetime.now().isoformat(),
      'total_repositories': len(repos_data),
      'total_organizations': repo_df['organization'].nunique(),
      'organizations': repo_df['organization'].unique().tolist(),
      'total_stars': int(repo_df['stars'].sum()),
      'total_forks': int(repo_df['forks'].sum()),
      'total_open_issues': int(repo_df['open_issues'].sum()),
      'average_stars_per_repo': round(repo_df['stars'].mean(), 2),
      'average_forks_per_repo': round(repo_df['forks'].mean(), 2),
      'organization_statistics': org_stats.to_dict(),
      'top_languages': language_counts,
      'top_repositories_by_stars': top_starred,
      'top_repositories_by_forks': top_forked,
      'contribution_statistics': {
        'total_contributors_analyzed': total_contributors,
        'average_contributors_per_repo': round(avg_contributors, 2),
        'top_contributors': [
          {
            'login': login,
            'total_contributions': data['total_contributions'],
            'repos_contributed': data['repos_contributed'],
            'avatar_url': data['avatar_url'],
            'html_url': data['html_url']
          }
          for login, data in top_contributors
        ]
      }
    }

    return summary

  def validate_data_quality(self, data: List[Dict]) -> Dict[str, Any]:
    """
    Validate the quality of collected data

    Args:
        data: List of repository data dictionaries

    Returns:
        Data quality report
    """
    if not data:
      return {'status': 'error', 'message': 'No data provided'}

    df = pd.DataFrame(data)

    quality_report = {
      'total_records': len(df),
      'missing_data': df.isnull().sum().to_dict(),
      'data_types': df.dtypes.astype(str).to_dict(),
      'duplicates': df.duplicated().sum(),
      'date_range': {
        'earliest_created': df['created_at'].min() if 'created_at' in df.columns else None,
        'latest_updated': df['updated_at'].max() if 'updated_at' in df.columns else None
      }
    }

    return quality_report
