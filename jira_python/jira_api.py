import json
from typing import Union, List, Dict

from jira import JIRA, Issue as JiraIssue

from data_utils import IssueDetails, IssueShort


BOARDS_ID = {
    "Platform board": 180,
    "Platform Mobile": 198
}


NO_EPIC_NAME = "NoEpic"


class JiraSession:
    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls._get_auth_jira()
        return cls._instance

    @staticmethod
    def _get_auth_jira(dirname="secret") -> JIRA:
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


def get_board_id(name: str) -> int:
    for board in JiraSession.get().boards():
        if board.name == name:
            return board.id
    raise ValueError("Board name not found")


def save_fields_id_to_name(filename: str = 'fields_id_to_name.json'):
    fields = JiraSession.get().fields()
    id_to_name = {field['id']: field['name'] for field in fields}
    with open(filename, 'w') as f:
        json.dump(id_to_name, f)


def get_epic_name(epic_key: str) -> str:
    jira_epic = JiraSession.get().issue(epic_key)
    epic_name = IssueDetails.get_epic_name(jira_epic)
    return epic_name


def fill_issue_epic_name(issue: IssueDetails, cache: Dict[str, str]) -> None:
    epic_key = issue.epic
    if epic_key is None:
        issue.epic = NO_EPIC_NAME
        issue.epic_name = NO_EPIC_NAME
    else:
        epic_name = cache.get(epic_key)
        if epic_name is None:
            epic_name = get_epic_name(epic_key)
            cache[epic_key] = epic_name
        issue.epic_name = epic_name


def get_issue(key: Union[str, IssueShort]) -> IssueDetails:
    if isinstance(key, IssueShort):
        key = key.key
    jira_issue = JiraSession.get().issue(key)
    issue = IssueDetails.from_jira_issue(jira_issue)
    fill_issue_epic_name(issue, cache=dict())
    return issue


def mark_sub_tasks_with_epic(issues: List[IssueDetails]) -> None:
    issues_map = {i.key: i for i in issues}
    for issue in issues:
        for sub_task in issue.sub_tasks:
            sub_task_issue = issues_map.get(sub_task.key)
            if sub_task_issue is not None:
                sub_task_issue.epic = issue.epic
                sub_task_issue.epic_name = issue.epic_name


def search_issues(project=None, component=None, sprint=None, epic=None, only_active=False, max_results=50):
    # eg. project='SP', component='Delta', sprint='Delta 2020 week 12', epic='blueZ'

    conditions = []
    if only_active:
        conditions.append('resolved=null')
    if project is not None:
        conditions.append(f'project="{project}"')
    if component is not None:
        conditions.append(f'component="{component}"')
    if sprint is not None:
        conditions.append(f'sprint="{sprint}"')
    if epic is not None:
        conditions.append(f'"Epic Link"="{epic}"')

    query = ' and '.join(conditions)
    issues = [IssueDetails.from_jira_issue(i) for i in JiraSession.get().search_issues(query, maxResults=max_results)]

    epics_cache = dict()
    for i in issues:
        fill_issue_epic_name(i, cache=epics_cache)

    mark_sub_tasks_with_epic(issues)
    return issues


def test():
    issues = search_issues(sprint='Delta 2020 week 12')
    for i in issues:
        print(i)

    print()
    issue = get_issue('SP-7140')
    print(issue)


if __name__ == "__main__":
    test()
