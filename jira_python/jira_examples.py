from jira import JIRA


BOARDS_ID = {
    "Platform board": 180,
    "Platform Mobile": 198
}


def get_board_id(jira: JIRA, name: str):
    for board in jira.boards():
        if board.name == name:
            return board.id
    raise ValueError("Board name not found")


def get_auth_jira(dirname="secret"):
    with open(f"{dirname}/jira_user", "r") as f:
        user = f.read()
    with open(f"{dirname}/jira_token", "r") as f:
        token = f.read()
    with open(f"{dirname}/jira_server", "r") as f:
        server = f.read()

    options = {
        'server': server
    }

    jira = JIRA(options, basic_auth=(user, token))
    return jira


def get_issue_info(issue, name_to_id):
    issue_keys = [
        "Key",
        "Summary",
        "Status",
        "Resolved",
        "Issue Type",
        "Story Points",
        "Project",
        "Labels",
        "Parent Link",
        "Linked Issues",
        "Assignee",
        "Components",
        "Sprint",
        "Epic Link",
        "Sub-tasks",
    ]
    fields = issue.raw['fields']
    result = {k: fields.get(name_to_id[k]) for k in issue_keys}
    result['Key'] = issue.key
    return result


def search_issues(jira: JIRA):
    for issue in jira.search_issues('project=SP and component=Delta and sprint="Delta 2020 week 12" and "Epic Link"="blueZ"', maxResults=50):
        print('{}: {}'.format(issue.key, issue.fields.summary))

    print(jira.project_components("SP"))
    print(jira.sprints(board_id=180, state="active"))


def test():
    jira = get_auth_jira()

    fields = jira.fields()
    name_to_id = {field['name']: field['id'] for field in fields}
    id_to_name = {field['id']: field['name'] for field in fields}

    ticket = 'SP-7076'
    issue = jira.issue(ticket)
    summary = issue.fields.summary

    for k, v in get_issue_info(issue, name_to_id).items():
        print(k, v)

    search_issues(jira)


if __name__ == "__main__":
    test()
