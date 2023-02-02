import os

from prefect import flow, task
from prefect.filesystems import GitHub


@flow
def clone_from_github(from_path, local_path):
    os.makedirs(local_path, exist_ok=True)
    github_block = GitHub.load("hw2ques4")
    github_block.get_directory(
        from_path=from_path,
        local_path=local_path
    )
    

if __name__ == '__main__':
    from_path = './cohorts/2023/week_2_workflow_orchestration/solution/question_1/'
    local_path = './flows'
    clone_from_github(from_path, local_path)