"""
GitHub Repository Data Collection Script for GitHub Actions
"""
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from github import Github
from github.Repository import Repository


class GitHubRepoCollector:
  """Collect GitHub repository data"""

  def __init__(self, github_token: str):
    self.github = Github(github_token)
    self.rate_limit_check()

  def rate_limit_check(self):
    """Check GitHub API rate limit"""
    rate_limit = self.github.get_rate_limit()
    remaining = rate_limit.core.remaining
    limit = rate_limit.core.limit

    print(f"GitHub API Rate Limit: {remaining}/{limit}")

    if remaining < 100:
      print("⚠️  WARNING: Low rate limit remaining!")
      return False
    return True

  def collect_organization_repos(self, org_name: str, max_repos: int = 50) -> List[Dict[str, Any]]:
    """Collect repositories from a GitHub organization"""
    try:
      print(f"🔍 Collecting repositories from organization: {org_name}")
      org = self.github.get_organization(org_name)
      repos = list(org.get_repos(type='public'))[:max_repos]

      repo_data = []
      for i, repo in enumerate(repos, 1):
        print(f"  📊 Processing {i}/{len(repos)}: {repo.name}")

        data = {
          'organization': org_name,
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
          'license': repo.license.name if repo.license else None,
          'topics': repo.get_topics(),
        }
        repo_data.append(data)

        # Check rate limit periodically
        if i % 20 == 0:
          self.rate_limit_check()

      print(f"✅ Collected {len(repo_data)} repositories from {org_name}")
      return repo_data

    except Exception as e:
      print(f"❌ Error collecting data from {org_name}: {str(e)}")
      return []


def main():
  parser = argparse.ArgumentParser(description='Collect GitHub repository data')
  parser.add_argument('--organizations', required=True, help='Comma-separated list of organizations')
  parser.add_argument('--max-repos', type=int, default=50, help='Maximum repositories per organization')
  parser.add_argument('--output-dir', required=True, help='Output directory')
  parser.add_argument('--date', required=True, help='Collection date (YYYY-MM-DD)')

  args = parser.parse_args()

  # Get GitHub token from environment
  github_token = os.getenv('GITHUB_TOKEN')
  if not github_token:
    print("❌ GITHUB_TOKEN environment variable not set")
    sys.exit(1)

  # Parse organizations
  orgs = [org.strip() for org in args.organizations.split(',')]
  print(f"🎯 Target organizations: {', '.join(orgs)}")

  # Initialize collector
  collector = GitHubRepoCollector(github_token)

  # Collect data from all organizations
  all_repo_data = []
  for org in orgs:
    repo_data = collector.collect_organization_repos(org, args.max_repos)
    all_repo_data.extend(repo_data)

  # Add collection metadata
  for repo in all_repo_data:
    repo['collection_date'] = args.date
    repo['collection_timestamp'] = datetime.now().isoformat()

  # Save to file
  output_file = Path(args.output_dir) / f"repos_raw_{args.date}.json"
  output_file.parent.mkdir(parents=True, exist_ok=True)

  with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(all_repo_data, f, indent=2, ensure_ascii=False, default=str)

  print(f"💾 Saved {len(all_repo_data)} repositories to {output_file}")
  print(f"📊 Total organizations processed: {len(orgs)}")

  # Final rate limit check
  collector.rate_limit_check()


if __name__ == '__main__':
  main()
