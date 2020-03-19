import json
from typing import Union

from jira import JIRA

from data_utils import IssueDetails, IssueShort


BOARDS_ID = {
    "Platform board": 180,
    "Platform Mobile": 198
}


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
    return [IssueDetails.from_jira_issue(i) for i in JiraSession.get().search_issues(query, maxResults=max_results)]


def get_issue(key: Union[str, IssueShort]) -> IssueDetails:
    if isinstance(key, IssueShort):
        key = key.key
    issue = JiraSession.get().issue(key)
    return IssueDetails.from_jira_issue(issue)


def test():
    issues = search_issues(sprint='Delta 2020 week 12')
    for i in issues:
        print(i)


if __name__ == "__main__":
    test()
