from oncallninja_integrations.sentry import SentryAPIClient
if __name__=="__main__":
    sentry_client = SentryAPIClient(auth_token="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",
                                    organization_slug="emitrr-w1")
    
    
    # print(sentry_client.get_organization())
    from datetime import datetime, timedelta

    # last_week = datetime.now() - timedelta(days=7)

    # projects = sentry_client.get_projects()

    # print(projects)

    # from datetime import datetime, timedelta

    start_date = datetime.now() - timedelta(hours=5)
    end_date = datetime.now()

    issues = sentry_client.get_issues_with_timestamps(
        project_slug="botwebhook",
    )

    print(issues)

    issue_details = sentry_client.get_issue_details(issue_id='4523282106')

    print(issue_details)

    # event = sentry_client.get_event(event_id='5f03ac6724dd42fdb9b9e301a1604e5c', project_slug='python-gcpfunctions')
    # print(event)

    # stack_trace = sentry_client.get_formatted_stack_trace(issue_id='6377844533')
    # print(stack_trace)