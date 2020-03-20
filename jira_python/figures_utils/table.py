from typing import List, Any, Dict

from data_utils import IssueDetails


def generate_table(issues: List[IssueDetails]) -> Dict[str, Any]:
    headers = ['key', 'summary', 'status', 'story points', 'epic', 'component', 'sprint']

    col_key = [i.key for i in issues]
    col_summary = [i.summary for i in issues]
    col_status = [i.status.name for i in issues]
    col_story_points = [i.story_points for i in issues]
    col_epic = [i.epic_name for i in issues]
    col_component = [i.main_component for i in issues]
    col_sprint = [i.active_sprint_name for i in issues]

    data = dict(
        type='table',
        header=dict(values=headers),
        cells=dict(values=[
            col_key,
            col_summary,
            col_status,
            col_story_points,
            col_epic,
            col_component,
            col_sprint,
        ]),
    )

    layout = dict(
        title="Tasks"
    )

    figure = dict(
        data=[data],
        layout=layout
    )
    return figure
