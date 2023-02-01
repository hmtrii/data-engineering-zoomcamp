from prefect.filesystems import GitHub

github_block = GitHub.load("hw2ques4")
github_block.get_directory()