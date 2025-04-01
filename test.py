from oncallninja_integrations.bitbucket import BitbucketClient, BitbucketConfig

def main():
    # Example token mapping
    token_mapping = {
        "horus-ai-labs/distillflow": "Nope, don't even think about it",
        "horus-ai-labs-2/DistillFlow2": "Seriously?"
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



if __name__ == "__main__":
    main()
