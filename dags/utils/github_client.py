"""
GitHub API Client for data collection
"""
from github import Github
from github.Repository import Repository
from github.NamedUser import NamedUser
from typing import List, Optional
import time
import logging


class GitHubDataCollector:
  """GitHub API client for collecting organization and repository data"""

  def __init__(self, github_token: str):
    """Initialize GitHub client with authentication token"""
    self.github = Github(github_token)
    self.logger = logging.getLogger(__name__)

    # Check rate limit
    rate_limit = self.github.get_rate_limit()
    self.logger.info(f"GitHub API rate limit: {rate_limit.core.remaining}/{rate_limit.core.limit}")

  def get_organization_repos(self, org_name: str) -> List[Repository]:
    """
    Get all repositories for a given organization

    Args:
        org_name: Name of the GitHub organization

    Returns:
        List of Repository objects
    """
    try:
      org = self.github.get_organization(org_name)
      repos = list(org.get_repos(type='public'))
      self.logger.info(f"Found {len(repos)} repositories for organization {org_name}")
      return repos
    except Exception as e:
      self.logger.error(f"Error fetching repos for organization {org_name}: {str(e)}")
      return []

  def get_repository_contributors(self, repo_full_name: str) -> List[NamedUser]:
    """
    Get contributors for a specific repository

    Args:
        repo_full_name: Full name of repository (org/repo)

    Returns:
        List of NamedUser objects (contributors)
    """
    try:
      repo = self.github.get_repo(repo_full_name)
      contributors = list(repo.get_contributors())
      self.logger.info(f"Found {len(contributors)} contributors for {repo_full_name}")
      return contributors
    except Exception as e:
      self.logger.error(f"Error fetching contributors for {repo_full_name}: {str(e)}")
      return []

  def get_repository_languages(self, repo_full_name: str) -> dict:
    """
    Get programming languages used in a repository

    Args:
        repo_full_name: Full name of repository (org/repo)

    Returns:
        Dictionary of languages and their usage
    """
    try:
      repo = self.github.get_repo(repo_full_name)
      languages = repo.get_languages()
      return languages
    except Exception as e:
      self.logger.error(f"Error fetching languages for {repo_full_name}: {str(e)}")
      return {}

  def check_rate_limit(self):
    """Check and log current rate limit status"""
    rate_limit = self.github.get_rate_limit()
    core = rate_limit.core

    self.logger.info(f"Rate limit status: {core.remaining}/{core.limit}")

    if core.remaining < 100:
      reset_time = core.reset
      wait_time = (reset_time - time.time()) + 60  # Add 1 minute buffer
      self.logger.warning(f"Rate limit low. Waiting {wait_time} seconds")
      time.sleep(max(0, wait_time))
