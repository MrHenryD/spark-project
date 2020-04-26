from jobs.batch import job

jobs = [job]

def run_jobs():
	for process in jobs:
		_ = process()

if __name__ == '__main__':
	_ = run_jobs()