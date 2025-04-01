import os
import requests
from oncallninja_integrations.bitbucket import BitbucketClient, BitbucketConfig

def main():

    # Get both Bitbucket access tokens
    token_1 = os.getenv("BITBUCKET_TOKEN_1")
    token_2 = os.getenv("BITBUCKET_TOKEN_2")
    
    token_mapping = {
        "horus-ai-labs/distillflow": token_1,
        "horus-ai-labs-2/DistillFlow2": token_2
    }
    
    # Configure the agent with token mapping
    bitbucket_config = BitbucketConfig(
        access_tokens=token_mapping
    )

    # Create and run the agent
    client = BitbucketClient(bitbucket_config)
    
    # Example usage
    print("=====================================================================")
    print("Workspaces:", client.list_workspaces())
    print("=====================================================================")
    print("Repositories:", client.list_repositories("horus-ai-labs"))
    print("=====================================================================")
    print("Repository Details:", client.get_repository(None, "horus-ai-labs/distillflow"))
    print("Repositories:", client.list_repositories("horus-ai-labs-2"))
    print("=====================================================================")
    print("Repository Details:", client.get_repository(None, "horus-ai-labs-2/DistillFlow2"))


def test_create_pr():
    token = os.getenv("BITBUCKET_TOKEN_3")
    if not token:
        raise ValueError("BITBUCKET_TOKEN_3 environment variable is not set")
        
    repo_name = "horus-ai-labs/distillflow"
    token_mapping = {
        repo_name: token,
    }
    
    # Configure the agent with token mapping
    config = BitbucketConfig(
        access_tokens=token_mapping
    )
    client = BitbucketClient(config)

    try:
        # Verify repository access first
        repo_details = client.get_repository(None, repo_name)
        print("Repository Details:", repo_details)

        # Get repository branches to verify source branch exists
        branches_response = requests.get(
            f"{config.api_url}/repositories/{repo_name}/refs/branches",
            headers=client._get_headers_for_repo(repo_name)
        )
        branches_response.raise_for_status()
        branches = branches_response.json()
        print("\nAvailable branches:")
        for branch in branches.get('values', []):
            print(f"- {branch.get('name')}")

        # Create the pull request using full repo name format
        pr_result = client.create_pull_request(
            workspace=None,  # Not needed since we're using full repo name
            repo_name=repo_name,  # Using full repo name format
            source_branch="pr-bitbucket",
            destination_branch="main",
            title="Test Pull Request",
            description="This is a test pull request created via the API",
            reviewers=None,
            close_source_branch=True
        )
        
        # Print the result
        print("Pull Request created successfully!")
        print(f"PR ID: {pr_result.get('id')}")
        print(f"Title: {pr_result.get('title')}")
        print(f"State: {pr_result.get('state')}")
        print(f"URL: {pr_result.get('url')}")
        
    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error creating pull request: {e}")


if __name__ == "__main__":
    test_create_pr()
