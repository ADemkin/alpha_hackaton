This folder contains sample solution

Files:

    run_client.py - runnable client with simple model (current volatility as predicted)
    hackathon_protocol.py - implementation of net protocol to interact with check_solution_server.py.
        To use it
            import hackathon_protocol
        Make sure, that file is in current directory

    Your prediction model should be developed in run_client.py.

How to start on local machine:
    1. Start check_solution_server.py in parent folder, wait until 'Server listening on port N' message appeared
    2. Start run_client.py, after finish check output

    Completed! items_processed: 99833, time_elapsed: 7.943 sec, score: 0.217

How to run with docker:
    1. Make sure check_solution_server.py is running
    2. Start run_solution_in_docker.py <directory with solution>

How to submit:
    TODO